select distinct
    enroll_info.enroll_id,
    fetal_center_pregnancy_all.mrn,
    fetal_center_pregnancy_all.patient_name,
    fetal_center_pregnancy_all.dob,
    research_study.res_stdy_id,
    enroll_info.study_alias,
    research_study.res_stdy_nm as study_name,
    dict_res_stat.dict_nm as study_status,
    enroll_info.enroll_start_dt,
    enroll_info.enroll_end_dt,
    provider.full_nm as prov_name,
    provider.prov_key,
    stg_patient_ods.patient_key
from
    {{ref('fetal_center_pregnancy_all')}} as fetal_center_pregnancy_all
    inner join {{ref('stg_patient_ods')}} as stg_patient_ods
        on stg_patient_ods.mrn = fetal_center_pregnancy_all.mrn
    inner join {{source('workday_ods', 'enroll_info')}} as enroll_info
        on enroll_info.pat_id = stg_patient_ods.pat_id
    inner join {{source('cdw', 'research_study')}} as research_study
        on research_study.res_stdy_id = enroll_info.research_study_id
    left join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = research_study.prov_key
    left join {{source('cdw', 'cdw_dictionary')}} as dict_res_stat
        on dict_res_stat.dict_key = research_study.dict_res_stat_key
        