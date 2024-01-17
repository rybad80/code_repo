{{ config(meta = {
    'critical': true
}) }}

select
    stg_surgery_log.or_key,
    stg_surgery_log.case_key,
    stg_surgery_log.log_key,
    'Completed' as case_status,
    stg_surgery_log.case_id,
    stg_surgery_log.log_id,
    stg_surgery_log.surgery_date,
    stg_surgery_log.dict_pat_class_key,
    stg_surgery_log.dict_or_svc_key,
    stg_surgery_log.dict_or_asa_rating_key,
    stg_surgery_log.loc_key,
    stg_surgery_log.room_prov_key,
    stg_surgery_log.source_system,
    stg_surgery_log.patient_name,
    stg_surgery_log.mrn,
    stg_surgery_log.dob,
    stg_surgery_log.sex,
    stg_surgery_log.pat_key,
    stg_surgery_log.csn,
    stg_surgery_log.surgery_csn,
    stg_surgery_log.encounter_date,
    stg_surgery_log.hospital_discharge_date,
    stg_hsp_acct_xref.hsp_acct_patient_class,
    stg_surgery_log.primary_surgeon,
    stg_surgery_log.posted_ind,
    stg_surgery_log.dict_or_case_type_key,
    stg_surgery_log.dict_or_case_class_key,
    stg_surgery_log.surgeon_prov_key,
    stg_surgery_log.visit_key,
    stg_surgery_log.encounter_key,
    stg_surgery_log.vsi_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    {{ ref('stg_surgery_log') }} as stg_surgery_log
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.visit_key = stg_surgery_log.admit_visit_key

union all

select
    stg_surgery_case.or_key,
    stg_surgery_case.case_key,
    null as log_key,
    'Scheduled' as case_status,
    stg_surgery_case.case_id,
    null as log_id,
    stg_surgery_case.surgery_date,
    stg_surgery_case.dict_pat_class_key,
    stg_surgery_case.dict_or_svc_key,
    -2 as dict_or_asa_rating_key,
    stg_surgery_case.loc_key,
    stg_surgery_case.room_prov_key,
    stg_surgery_case.source_system,
    stg_surgery_case.patient_name,
    stg_surgery_case.mrn,
    stg_surgery_case.dob,
    stg_surgery_case.sex,
    stg_surgery_case.pat_key,
    stg_surgery_case.csn,
    stg_surgery_case.surgery_csn,
    stg_surgery_case.encounter_date,
    stg_surgery_case.hospital_discharge_date,
    stg_hsp_acct_xref.hsp_acct_patient_class,
    stg_surgery_case.primary_surgeon,
    0 as posted_ind,
    null as dict_or_case_type_key,
    null as dict_or_case_class_key,
    stg_surgery_case.surgeon_prov_key,
    stg_surgery_case.visit_key,
    stg_surgery_case.encounter_key,
    null as vsi_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    {{ ref('stg_surgery_case') }} as stg_surgery_case
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.visit_key = stg_surgery_case.admit_visit_key
where
    sched_dt >= current_date
    and case_status = 1 -- 'Scheduled'
