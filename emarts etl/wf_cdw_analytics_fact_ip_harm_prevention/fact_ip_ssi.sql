with surv_class_theradoc as (
    select
        inf_surv_key,
        max(case when inf_surv_cls_nm like '%BONE%' then 'BONE'
                   when inf_surv_cls_nm like '%BRST%' then 'BREAST'
                   when inf_surv_cls_nm like '%CARD%' then 'CARD'
                   when inf_surv_cls_nm like '%DISC%' then 'DISC SPACE'
                   when inf_surv_cls_nm like '%EAR%' then 'EAR'
                   when inf_surv_cls_nm like '%EMET%' then 'ENDOMETRITIS'
                   when inf_surv_cls_nm like '%ENDO%' then 'ENDOCARDITIS'
                   when inf_surv_cls_nm like '%EYE%' then 'EYE'
                   when inf_surv_cls_nm like '%GIT%' then 'GI TRACT'
                   when inf_surv_cls_nm like '%HEP%' then 'HEPATITIS'
                   when inf_surv_cls_nm like '%IAB%' then 'INTRAABDOMINAL'
                   when inf_surv_cls_nm like '%IC%' then 'INTRACRANIAL'
                   when inf_surv_cls_nm like '%JNT%' then 'JOINT OR BURSA'
                   when inf_surv_cls_nm like '%LUNG%' then 'LUNG'
                   when inf_surv_cls_nm like '%MED%' then 'MEDIASTINITIS'
                   when inf_surv_cls_nm like '%MEN%' then 'MENINGITIS'
                   when inf_surv_cls_nm like '%ORAL%' then 'ORAL CAVITY'
                   when inf_surv_cls_nm like '%OREP%' then 'REPRODUCTIVE TRACT'
                   when inf_surv_cls_nm like '%OUTI%' then 'URINARY TRACT'
                   when inf_surv_cls_nm like '%Spinal abscess without meningitis%' then 'SPINAL ABSCESS WITHOUT MENINGITIS'
                   when inf_surv_cls_nm like '%SINU%' then 'SINUSITIS'
                   when inf_surv_cls_nm like '%UR%' then 'UPPER RESPIRATORY TRACT'
                   when inf_surv_cls_nm like '%VASC%' then 'VENOUS INFECTION'
                   when inf_surv_cls_nm like '%VCUF%' then 'VAGINAL CUFF'
              end) as infection_loc,
        max(case when inf_surv_cls_nm like '%SIP%' then 'Superficial'
                    when inf_surv_cls_nm like '%SIS%' then 'Superficial'
                    when inf_surv_cls_nm like '%DIP%' then 'Deep'
                    when inf_surv_cls_nm like '%DIS%' then 'Deep'
                    when inf_surv_cls_nm like '%Organ/space%' then 'Organ Space'
              end) as ssi_case_type,
        max(case when lower(inf_surv_cls_nm) like '%infection present at time of surgery%' then inf_surv_cls_ansr else 'N' end) as patos,
        max(case when inf_surv_cls_ansr like '%RF%' then 1 else 0 end) as detected_during_readmission_ind,
        min(seq_num) as ranking

    from {{ source('cdw', 'infection_surveillance_class') }}

    where create_by = 'THERADOC'

    group by inf_surv_key
),

surv_class_bugsy as (
    select
        class.inf_surv_key,
        null as infection_loc,
        null as ssi_case_type,
        max(case when bugsy_custom_infection_classes.ssi_patos_yn = 'Y' then 'Y' else 'N' end) as patos,
        max(case when bugsy_custom_infection_classes.ssi_detected_during in (3, 4) then 1 else 0 end) as detected_during_readmission_ind,
        min(seq_num) as ranking

    from
        {{ source('cdw', 'infection_surveillance_class') }} as class
        left join {{ source('cdw', 'infection_surveillance') }} as infection_surveillance
            on class.inf_surv_key = infection_surveillance.inf_surv_key
        left join {{ref('bugsy_custom_infection_classes')}} as bugsy_custom_infection_classes
            on infection_surveillance.inf_surv_id = bugsy_custom_infection_classes.c54_td_ica_surv_id

    where
        class.create_by = 'BUGSY'

    group by
        class.inf_surv_key
),

surv_class as (
    select * from surv_class_bugsy
    union all
    select * from surv_class_theradoc
),

surv_surg as (
    select
        inf_surv_key,
        seq_num,
        log_key,
        or_seq_num,
        or_proc_key,
        nhsn_cat_nm,
        surg_desc,
        case when surg_rank is not null then 1 else surg_rank end as surg_rank
    from (
        select
            inf_surv_key,
            seq_num,
            log_key,
            or_seq_num,
            or_proc_key,
            surg_rank,
            nhsn_cat_nm,
            surg_desc,
            row_number() over (partition by inf_surv_key order by surg_rank asc) as surg_rownum
        from {{ source('cdw', 'infection_surveillance_surgery') }}
    ) as x --noqa: L025
    where surg_rownum = 1
),
discharge_location as (
    select
        vai.visit_key,
        coalesce(m.historical_dept_key, d.dept_key) as dept_key,
        case when m.historical_dept_key is not null then m.historical_dept_abbr else d.dept_abbr end as dept_abbr,
        case when m.historical_dept_key is not null then m.historical_dept_id else d.dept_id end as dept_id
    from
        {{ source('cdw', 'visit_addl_info') }} as vai
        inner join {{ source('cdw', 'visit_event') }} as ve on vai.disch_visit_event_key = ve.visit_event_key
        inner join {{ source('cdw', 'department') }} as d on ve.dept_key = d.dept_key
        left join {{ ref('master_harm_prevention_dept_mapping') }} as m on m.harm_type = 'SSI' and m.current_dept_id = d.dept_id and ve.eff_event_dt between m.start_dt and m.end_dt
),
surv_order_organism as (
        select
            inf_surv_key,
            max(case when rn = 1 then organism_nm end) as organism_01,
            max(case when rn = 2 then organism_nm end) as organism_02,
            max(case when rn = 3 then organism_nm end) as organism_03
        from (
            select
                inf_surv_key,
                organism_nm,
                row_number() over (partition by inf_surv_key order by
                                                case when inf_micro_nm like '%PCR%' then -200 + micro_rank
                                                     when inf_micro_nm like '%CULTURE%' then -100 + micro_rank
                                                     else micro_rank
                                                end asc
                ) as rn
            from {{ source('cdw', 'infection_surveillance_micro') }}
        ) as y --noqa: L025
        group by inf_surv_key
),
distinct_surgeons as (
    select distinct
        log_key,
        surg_prov_key,
        dict_or_role_key,
        dict_or_svc_key,
        panel_num
    from {{ source('cdw', 'or_log_surgeons') }}
),
-- detected_during_readmission as (
--     select
--         inf_surv_key,
--         max(case when inf_surv_cls_ansr like '%RF%' then 1 else 0 end) as detected_during_readmission_ind
--     from {{ source('cdw', 'infection_surveillance_class') }}
--     group by inf_surv_key
-- ),
international as (
    select
        pat_key,
        max(international_ind) as international_ind
    from {{ ref('stg_visit_event_service') }}
    group by pat_key
),
theradoc as (
    select
          coalesce(h.eventid, -1) as hai_event_id,
        coalesce(orlog.log_key, -1) as log_key,
        coalesce(visit.visit_key, -1) as visit_key,
        coalesce(h.pat_key, -1) as pat_key,
        coalesce(surgeon.prov_key, -1) as surg_prov_key,
        coalesce(orroom.prov_key, -1) as or_room_prov_key,
        coalesce(m_dept.historical_dept_key, coalesce(dept.dept_key, -1)) as dept_key,
        coalesce(discharge_location.dept_key, -1) as dischrg_dept_key,
        coalesce(surv.inf_surv_key, -1) as inf_surv_key,
        coalesce(orproc.or_proc_key, -1) as or_proc_key,
        surv.inf_surv_id,
        case when m_dept.historical_dept_key is not null then m_dept.historical_dept_id else coalesce(dept.dept_id, -1) end as dept_id,
        discharge_location.dept_id as dischrg_dept_id,
        visit.enc_id,
        p.pat_mrn_id,
        p.last_nm as pat_last_nm,
        p.first_nm as pat_first_nm,
        p.full_nm as pat_full_nm,
        p.dob as pat_dob,
        p.sex as pat_sex,
        p.country as pat_country,
        surgeon.full_nm as surgeon_nm,
        case when m_dept.historical_dept_key is not null then m_dept.historical_dept_nm else dept.dept_nm end as dept_nm,
        discharge_location.dept_abbr as dischrg_dept_abbr,
        orproc.or_proc_nm,
        orproc.or_proc_id,
        orroom.full_nm as or_room_nm,
        coalesce(zc_orp_rpt_grp_s2.name, 'NOT APPLICABLE') as nhsn_category,
        surv_class.infection_loc,
        case when lower(dictservice.dict_nm) in ('transplant', 'fetal surgery') then 'General Surgery' else dictservice.dict_nm end as division,
        allproc.seq_num as proc_seq_num,
        surv_class.patos,
        surv_class.ssi_case_type,
        dictwoundclass.dict_nm as wound_class,
        case when max(postop.incis_comp_close_ind) over (partition by surv.inf_surv_id) = 0 then 'Open' else 'Closed' end as closure_technique,
        survorg.organism_01,
        survorg.organism_02,
        survorg.organism_03,
        h.pathogen1 as pathogen_code_1,
        h.pathogendesc1 as pathogen_desc_1,
        h.pathogen2 as pathogen_code_2,
        h.pathogendesc2 as pathogen_desc_2,
        h.pathogen3 as pathogen_code_3,
        h.pathogendesc3 as pathogen_desc_3,
        date(surv.inf_dt) - surgdate.full_dt as num_days_inf_from_surgery,
        visit.hosp_admit_dt,
        visit.hosp_dischrg_dt,
        or_log_visit.hosp_admit_dt as surg_admt_dt,
        or_log_visit.hosp_dischrg_dt as surg_dischrg_dt,
        surgdate.full_dt as surg_dt,
        h.eventdate as event_dt,
        surv.conf_dt,
        coalesce(international.international_ind, -2) as international_ind,
        coalesce(surv_class.detected_during_readmission_ind, -2) as detected_during_readmission_ind,
        case when h.eventdate >= '2013-07-01' then 1 else 0 end as reportable_ind,
        surv_class.ranking
    from
        {{ source('cdw_analytics', 'metrics_hai') }} as h
        left join {{ source('cdw', 'patient') }} as p on p.pat_key = h.pat_key
        left join {{ source('cdw', 'infection_surveillance') }} as surv on surv.inf_surv_id = h.eventid
        inner join surv_class on surv_class.inf_surv_key = surv.inf_surv_key
        left join surv_surg as survsurg on surv.inf_surv_key = survsurg.inf_surv_key and survsurg.surg_rank = 1
        left join {{ source('cdw', 'infection_surveillance_visit') }} as survvisit on surv.inf_surv_key = survvisit.inf_surv_key and coalesce(survvisit.seq_num, 1) = 1
        left join {{ source('cdw', 'visit') }} as visit on visit.visit_key = survvisit.visit_key
        left join {{ source('cdw', 'department') }} as dept on dept.dept_key = surv.dept_key
        left join {{ ref('master_harm_prevention_dept_mapping') }} as m_dept
            on m_dept.harm_type = 'SSI'
            and m_dept.current_dept_id = dept.dept_id
            and h.eventdate between m_dept.start_dt and m_dept.end_dt
            and m_dept.denominator_only_ind = 0
        inner join {{ source('cdw', 'or_log') }} as orlog on survsurg.log_key = orlog.log_key
        inner join {{ source('cdw', 'or_log_all_procedures') }} as allproc on orlog.log_key = allproc.log_key and allproc.seq_num = survsurg.or_seq_num and allproc.or_proc_key = survsurg.or_proc_key
        inner join {{ source('cdw', 'or_procedure') }} as orproc on orproc.or_proc_key = allproc.or_proc_key
        inner join {{source('chop_analytics', 'stg_surgery_nhsn_procedure_codes_history')}} as stg_surgery_nhsn_procedure_codes_history on orproc.or_proc_id = stg_surgery_nhsn_procedure_codes_history.or_proc_id
        left join {{source('clarity_ods', 'zc_orp_rpt_grp_s2')}} as zc_orp_rpt_grp_s2 on stg_surgery_nhsn_procedure_codes_history.rpt_grp2_c = zc_orp_rpt_grp_s2.orp_rpt_grp_s2_c
        left join {{ source('cdw', 'infection_surveillance_micro') }} as survorder on survorder.inf_surv_key = surv.inf_surv_key
        inner join {{ source('cdw', 'master_date') }} as surgdate on surgdate.dt_key = orlog.surg_dt_key
        left join discharge_location on orlog.admit_visit_key = discharge_location.visit_key
        inner join distinct_surgeons on distinct_surgeons.log_key = orlog.log_key
        inner join {{ source('cdw', 'provider') }} as surgeon on surgeon.prov_key = distinct_surgeons.surg_prov_key
        inner join {{ source('cdw', 'provider') }} as orroom on orroom.prov_key = orlog.room_prov_key
        inner join {{ source('cdw', 'infection_surveillance_class') }} as infection_surveillance_class on surv.inf_surv_key = infection_surveillance_class.inf_surv_key
        left join surv_order_organism as survorg on survorg.inf_surv_key = surv.inf_surv_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dictwoundclass on dictwoundclass.dict_key = allproc.dict_wound_class_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as casestatus on casestatus.dict_key = orlog.dict_or_stat_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dictservice on dictservice.dict_key = distinct_surgeons.dict_or_svc_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dictanestype on dictanestype.dict_key = allproc.dict_or_anes_type_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dictorrole on dictorrole.dict_key = distinct_surgeons.dict_or_role_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dictsurgin on dictsurgin.dict_key = orproc.dict_rpt_grp4_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dictabxreq on dictabxreq.dict_key = orproc.dict_rpt_grp1_key
        inner join {{ source('cdw', 'or_log_case_times') }} as times on times.log_key = orlog.log_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as casetimes on times.dict_or_pat_event_key = casetimes.dict_key
        left join {{ source('cdw', 'visit') }} as or_log_visit on orlog.admit_visit_key = or_log_visit.visit_key
        left join {{ source('cdw', 'or_post_op_prep_info') }} as postop on orlog.log_key = postop.log_key
        left join international on international.pat_key = p.pat_key
   where
       h.hai_type = 'SSI'
       and date(surv.inf_dt) >= '2010-07-01'
       and date(surgdate.full_dt) >= '2010-07-01'
       and allproc.all_proc_panel_num = distinct_surgeons.panel_num
       and upper(dictorrole.dict_nm) = 'PRIMARY'
       and surv.work_status = 'COMPLETE'
       and surv.assigned_to_icp not like '%SIGN%'
       and surv.inf_acq_type = 'HOSPITAL-ASSOCIATED'
       and infection_surveillance_class.inf_surv_cls_nm in('SSI (surgical site infection)', 'Surgical Site Infection')
       and (surv_class.patos = 'N' or (surv_class.patos = 'Y' and date(surgdate.full_dt) < '2017-07-01'))
       and date(stg_surgery_nhsn_procedure_codes_history.dbt_valid_from) < '2024-01-03'
)
select distinct
 hai_event_id,
log_key,
visit_key,
pat_key,
surg_prov_key,
or_room_prov_key,
dept_key,
dischrg_dept_key,
inf_surv_key,
or_proc_key,
or_proc_id,
inf_surv_id,
cast(dept_id as bigint) as dept_id,
cast(dischrg_dept_id as bigint) as dischrg_dept_id,
cast(enc_id as numeric(14, 3)) as enc_id,
cast(pat_mrn_id as varchar(25)) as pat_mrn_id,
cast(pat_last_nm as varchar(50)) as pat_last_nm,
cast(pat_first_nm as varchar(50)) as pat_first_nm,
cast(pat_full_nm as varchar(200)) as pat_full_nm,
pat_dob,
pat_sex,
pat_country,
surgeon_nm,
dept_nm,
cast(dischrg_dept_abbr as varchar(300)) as dischrg_dept_abbr,
cast(or_proc_nm as varchar(200)) as or_proc_nm,
cast(or_room_nm as varchar(200)) as or_room_nm,
cast(nhsn_category as varchar(100)) as nhsn_category,
cast(infection_loc as varchar(100)) as infection_loc,
cast(division as varchar(50)) as division,
proc_seq_num,
cast(patos as varchar(100)) as patos,
cast(ssi_case_type as varchar(100)) as ssi_case_type,
cast(wound_class as varchar(100)) as wound_class,
cast(closure_technique as varchar(100)) as closure_technique,
organism_01 as organism_1,
organism_02 as organism_2,
organism_03 as organism_3,
pathogen_code_1,
pathogen_desc_1,
pathogen_code_2,
pathogen_desc_2,
pathogen_code_3,
pathogen_desc_3,
cast(num_days_inf_from_surgery as numeric(18)) as num_days_inf_from_surgery,
hosp_admit_dt,
hosp_dischrg_dt,
surg_admt_dt as surg_admit_dt,
surg_dischrg_dt,
cast(surg_dt as timestamp) as surg_dt,
event_dt,
conf_dt,
cast(international_ind as byteint) as international_ind,
cast(detected_during_readmission_ind as byteint) as detected_during_readmission_ind,
cast(reportable_ind as byteint) as reportable_ind,
current_timestamp as create_dt,
'DBT' as create_by,
current_timestamp as upd_dt
from theradoc
where ranking = 1
