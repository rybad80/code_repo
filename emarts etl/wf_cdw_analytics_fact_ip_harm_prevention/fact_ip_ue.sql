with corrections as (
    --region identify correct pat_keys and visit_keys
    select
        stg_harm_ue.record_id,
        COALESCE(visit_from_hosp_enc.pat_key,
            visit_from_nonhosp_enc.pat_key,
            visit_from_pat_key.pat_key,
            visit_from_name.pat_key,
            visit_from_dob.pat_key) as pat_key,
        COALESCE(visit_from_hosp_enc.visit_key,
            visit_from_nonhosp_enc.visit_key,
            visit_from_pat_key.visit_key,
            visit_from_name.visit_key,
            visit_from_dob.visit_key) as visit_key,
        /*Following variables renamed for prioritization within CTE*/
        COALESCE(visit_from_hosp_enc.dept_key,
            visit_from_nonhosp_enc.dept_key,
            visit_from_pat_key.dept_key,
            visit_from_name.dept_key,
            visit_from_dob.dept_key) as d_key,
        COALESCE(visit_from_hosp_enc.enter_dt,
            visit_from_nonhosp_enc.enter_dt,
            visit_from_pat_key.enter_dt,
            visit_from_name.enter_dt,
            visit_from_dob.enter_dt) as enter_date,
        COALESCE(visit_from_hosp_enc.exit_dt,
            visit_from_nonhosp_enc.exit_dt,
            visit_from_pat_key.exit_dt,
            visit_from_name.exit_dt,
            visit_from_dob.exit_dt) as exit_date,
        case --prioritize different event time situations
            when stg_harm_ue.event_dt + stg_harm_ue.event_tm between enter_date and exit_date --always returns one unit 
                then 1
            when stg_harm_ue.event_tm is null --can be multiple encounters
                then 2
            else 3 --event time before first hospital encounter; can also be multiple encounters
        end as priority_rank,
        COALESCE(visit_from_hosp_enc.adt_svc_nm,
            visit_from_nonhosp_enc.adt_svc_nm,
            visit_from_pat_key.adt_svc_nm,
            visit_from_name.adt_svc_nm,
            visit_from_dob.adt_svc_nm) as adt_svc_nm,
        stg_harm_ue.dept_name,
        stg_harm_ue.event_dt,
        stg_harm_ue.submit_dt,
        stg_harm_ue.event_tm,
        stg_harm_ue.procedural_area,
        stg_harm_ue.confirmed_not_ue,
        stg_harm_ue.numerator_source
    from
        /* Numerous joins created to fix flawed data
        * Order of joins is based on severity:
        * Least flawed to most flawed */
        {{ref('stg_harm_ue')}} as stg_harm_ue
        /*visit_key matches to a hospital encounter*/
        left join {{ref('stg_visit_event_service')}} as visit_from_hosp_enc
            on stg_harm_ue.visit_key = visit_from_hosp_enc.visit_key
            and stg_harm_ue.event_dt between DATE(visit_from_hosp_enc.enter_dt) and COALESCE(DATE(visit_from_hosp_enc.exit_dt), CURRENT_DATE)
        /*visit_key matches to an appointment on the same encounter*/
        left join {{ source('cdw','visit') }} as v_nonhosp_enc
            on stg_harm_ue.visit_key = v_nonhosp_enc.visit_key
        --link to hospital encounter
        left join {{ref('stg_visit_event_service')}} as visit_from_nonhosp_enc
            on v_nonhosp_enc.ip_documented_visit_key = visit_from_nonhosp_enc.visit_key
            and stg_harm_ue.event_dt between DATE(visit_from_nonhosp_enc.enter_dt) and COALESCE(DATE(visit_from_nonhosp_enc.exit_dt), CURRENT_DATE)
        /*pat_key is valid*/
        left join {{ref('stg_visit_event_service')}} as visit_from_pat_key
            on stg_harm_ue.pat_key = visit_from_pat_key.pat_key
            and stg_harm_ue.event_dt between DATE(visit_from_pat_key.enter_dt) and COALESCE(DATE(visit_from_pat_key.exit_dt), CURRENT_DATE)
        /*Name matches to existing alias*/
        left join {{ source('cdw', 'kaps_incident_1')}} as kaps_incident_1
            on stg_harm_ue.record_id = LPAD(TRIM(TRANSLATE(kaps_incident_1.file_id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()-,', '')), 15, '0')
            and stg_harm_ue.numerator_source = 'KAPS'
        left join {{ source('cdw', 'kaps_event_person_affected') }} as kaps_person
            on kaps_incident_1.incid_id = kaps_person.incid_id
        left join {{ source('cdw', 'patient_alias') }} as patient_alias
            on kaps_person.person_last_nm || ',' || kaps_person.person_first_nm = patient_alias.alias_desc
        left join {{ref('stg_visit_event_service')}} as visit_from_name
            on patient_alias.pat_key = visit_from_name.pat_key
            and stg_harm_ue.event_dt between DATE(visit_from_name.enter_dt) and COALESCE(DATE(visit_from_name.exit_dt), CURRENT_DATE)
        left join {{ source('cdw', 'patient') }} as pat_from_name
            on visit_from_name.pat_key = pat_from_name.pat_key
        /*DOB is valid, but not visit_key, pat_key, or Name*/
        left join {{ source('cdw', 'patient') }} as pat_from_dob
            on DATE(kaps_person.person_dob) = DATE(pat_from_dob.dob)
        left join {{ref('stg_visit_event_service')}} as visit_from_dob
            on pat_from_dob.pat_key = visit_from_dob.pat_key
            and stg_harm_ue.event_dt between DATE(visit_from_dob.enter_dt) and COALESCE(DATE(visit_from_dob.exit_dt), CURRENT_DATE)
    where
        --visit_key matches to a hospital encounter
        visit_from_hosp_enc.pat_key is not null
        --visit_key is for a different encounter type
        or visit_from_nonhosp_enc.pat_key is not null
        --pat_key is valid
        or visit_from_pat_key.pat_key is not null
        --Alias match to patient
        or LOWER(pat_from_name.last_nm) = LOWER(kaps_person.person_last_nm)
        --DOB match to patient
        or LOWER(pat_from_dob.last_nm) = LOWER(kaps_person.person_last_nm)
        --No match to patient, but self-match suggests a unique solution
        or pat_from_name.full_nm = pat_from_dob.full_nm
),
final_records as (
--CHOP ICU
select distinct
    corrections.record_id,
    corrections.pat_key,
    corrections.visit_key,
    corrections.event_dt,
    corrections.event_tm,
    corrections.submit_dt,
    /*Choose department if event date-time is within stay
    Choose first department if event time is null (same cost center)
    Choose first department visited if event time before first enter date
    */
    FIRST_VALUE(corrections.d_key) over(
        partition by
            corrections.record_id
        order by
            corrections.priority_rank,
            corrections.enter_date
    ) as dept_key,
--    d.department_center_abbr,  --removing to resolve dupes
    corrections.dept_name,
    corrections.adt_svc_nm,
    corrections.procedural_area,
    corrections.confirmed_not_ue,
    corrections.numerator_source,
    case
        when DENSE_RANK() over(
            partition by
                corrections.pat_key,
                corrections.visit_key,
                corrections.event_dt
            order by
                corrections.record_id desc
        ) = 1
        then 1 else 0 end as last_record_ind
from
    corrections
    inner join {{ref('fact_department_rollup')}} as d
        on corrections.d_key = d.dept_key
        and corrections.event_dt = d.dept_align_dt
where
    --NICU, CICU (OVF), PICU (OVF)
    SUBSTRING(d.unit_dept_grp_abbr, 1, 4) = corrections.dept_name
    or (
        corrections.dept_name = 'KOPH 2 PICU'
        and d.rollup_nm = 'KOPH 2 ICU'
    )
    or (
        corrections.dept_name = 'KOPH 5 NICU'
        and d.rollup_nm = 'KOPH 5 NICU'
    )
    or (
        corrections.dept_name = 'Procedural Area'
        and d.rollup_nm not like '%ICU%'
    )
)
select
    CAST(record_id as bigint) as record_id,
    CAST(final_records.pat_key as bigint) as pat_key,
    CAST(visit_key as bigint) as visit_key,
    p.pat_mrn_id as mrn,
    CAST(p.last_nm as varchar(200)) as last_nm,
    CAST(p.first_nm as varchar(200)) as first_nm,
    event_dt,
    event_tm,
    submit_dt,
    dept_key,
    dept_name,
    adt_svc_nm,
    procedural_area,
    confirmed_not_ue as ue_status,
    case when confirmed_not_ue = 'Yes'
        then 1 else 0 end as reportable_ind,
    numerator_source
from final_records
inner join {{source('cdw', 'patient')}} as p
    on p.pat_key = final_records.pat_key
where last_record_ind = 1
