{{ config(meta = {
    'critical': true
}) }}

with case_surgeon as (
    select
        or_case.or_case_key as case_key,
        provider.full_nm as primary_surgeon,
        provider.prov_key as surgeon_prov_key,
        row_number() over (
            partition by or_case.or_case_key order by or_case_all_surgeons.case_begin
        ) as surgeon_number
    from
        {{source('cdw', 'or_case')}} as or_case
        inner join {{source('cdw', 'or_case_all_surgeons')}} as or_case_all_surgeons
            on or_case_all_surgeons.or_case_key = or_case.or_case_key
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = or_case_all_surgeons.surg_prov_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_panel_role
            on dict_or_panel_role.dict_key = or_case_all_surgeons.dict_or_panel_role_key
    where
        dict_or_panel_role.src_id in (1.0000, 1.0030) --primary
        and or_case_all_surgeons.panel_num = 1
    group by
        or_case.or_case_key,
        provider.full_nm,
        provider.prov_key,
        or_case_all_surgeons.case_begin
)

select
    or_case.or_case_key as or_key,
    or_case.or_case_key as case_key,
    or_case.or_case_id as case_id,
    or_case.log_key,
    master_date.full_dt as surgery_date,
    or_case.dict_or_pat_class_key as dict_pat_class_key,
    or_case.dict_or_svc_key,
    or_case.loc_key,
    or_case.room_prov_key,
    or_case.create_by as source_system,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.sex,
    stg_patient.pat_key,
    stg_encounter.csn,
    cast(pat_or_adm_link.pat_enc_csn_id as numeric(14, 3)) as surgery_csn,
    stg_encounter.encounter_date,
    stg_encounter.hospital_discharge_date,
    case_surgeon.primary_surgeon,
    case_surgeon.surgeon_prov_key,
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    or_case.sched_dt,
    dict_sched_stat.src_id as case_status,
    or_case.add_on_case_ind,
    or_case.add_on_case_sch_ind,
    or_case.admit_visit_key
from
    {{source('cdw', 'or_case')}} as or_case
    inner join case_surgeon
        on case_surgeon.case_key = or_case.or_case_key
            and case_surgeon.surgeon_number = 1
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = or_case.pat_key
    left join  {{source('cdw', 'master_date')}} as master_date
        on master_date.dt_key = or_case.surg_dt_key
    left join {{ref('stg_encounter')}}  as stg_encounter
        on stg_encounter.visit_key = or_case.admit_visit_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_sched_stat
        on dict_sched_stat.dict_key = or_case.dict_or_sched_stat_key
    left join {{source('clarity_ods','pat_or_adm_link')}}  as pat_or_adm_link
        on pat_or_adm_link.case_id = or_case.or_case_id
