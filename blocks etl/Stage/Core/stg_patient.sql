{{ config(
    materialized='table',
    dist='pat_key',
    meta={
        'critical': true
    }
) }}


select
    coalesce(patient.pat_key, stg_patient_ods.patient_key) as pat_key,
    stg_patient_ods.*
from
    {{ref('stg_patient_ods')}} as stg_patient_ods
    left join {{source('cdw','patient')}} as patient
        on patient.pat_id = stg_patient_ods.pat_id
