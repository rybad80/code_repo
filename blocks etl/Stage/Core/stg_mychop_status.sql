{{ config(meta = {
    'critical': true
}) }}

select
    stg_patient.pat_key,
    stg_patient.patient_key,
    min(
        case when dim_patient_mychart_info_status.pat_mychart_info_stat_id = 1 then 1 else 0 end
    ) as mychop_activation_ind,
    min(
        case when dim_patient_mychart_info_status.pat_mychart_info_stat_id = 5 then 1 else 0 end
    ) as mychop_declined_ind
from
    {{ref('stg_patient')}} as stg_patient
    inner join {{source('cdw', 'patient_mychart_info')}} as patient_mychart_info
        on stg_patient.pat_key = patient_mychart_info.pat_key
    inner join {{source('cdw', 'dim_patient_mychart_info_status')}} as dim_patient_mychart_info_status
        on dim_patient_mychart_info_status.dim_pat_mychart_info_stat_key
        = patient_mychart_info.dim_pat_mychart_info_stat_key
group by
    stg_patient.pat_key,
    stg_patient.patient_key
