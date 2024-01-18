{{ config(meta = {
    'critical': true
}) }}

select
    patient_mychart_info.pat_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.sex,
    stg_patient.race_ethnicity,
    stg_patient.dob,
    dim_patient_mychart_info_status.pat_mychart_info_stat_nm as mychop_status,
    dim_patient_mychart_info_status.pat_mychart_info_stat_id as mychop_status_id,
    min(date(mychart_user_access.action_dt)) as first_login_date,
    max(date(mychart_user_access.action_dt)) as last_login_date,
    case
        when dim_patient_mychart_info_status.pat_mychart_info_stat_id = 1
        then 1
        else 0
    end as currently_active_ind,
    case when last_login_date is not null then 1 else 0 end as ever_used_ind,
    case when count(distinct mychart_user_access.session_num) > 1 then 1 else 0 end as multiple_use_ind,
    patient_mychart_info.mychart_user_key
from
    {{ref('stg_patient')}} as stg_patient
    inner join {{source('cdw', 'patient_mychart_info')}} as patient_mychart_info
        on patient_mychart_info.pat_key = stg_patient.pat_key
    left join {{source('cdw', 'dim_patient_mychart_info_status')}} as dim_patient_mychart_info_status
        on dim_patient_mychart_info_status.dim_pat_mychart_info_stat_key
        = patient_mychart_info.dim_pat_mychart_info_stat_key
    left join {{source('cdw', 'mychart_user_access')}} as mychart_user_access
        on mychart_user_access.mychart_user_key = patient_mychart_info.mychart_user_key
group by
    patient_mychart_info.pat_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.sex,
    stg_patient.race_ethnicity,
    stg_patient.dob,
    dim_patient_mychart_info_status.pat_mychart_info_stat_nm,
    dim_patient_mychart_info_status.pat_mychart_info_stat_id,
    patient_mychart_info.mychart_user_key
