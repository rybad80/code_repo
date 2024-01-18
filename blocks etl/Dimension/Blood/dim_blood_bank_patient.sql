{{
  config(
    materialized = 'table',
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = ['patient.patient_id', 'clarity_patient.mrn', 'patient.abo_cd', 'patient.rh_cd'] %}
with clarity_patient as (
    select
        stg_patient_ods.patient_key,
        stg_patient_ods.mrn
    from {{ref('stg_patient_ods')}} as stg_patient_ods
    where
        stg_patient_ods.current_record_ind = 1
),
unique_patients as (
    select
        patient_medical_record.medical_recno
    from {{source('safetrace_ods', 'safetrace_patient')}} as patient
    inner join {{source('safetrace_ods', 'safetrace_patient_medical_record')}} as patient_medical_record
        on patient_medical_record.patient_id = patient.patient_id
    where patient_medical_record.current_mrn = 'Y'
    group by patient_medical_record.medical_recno
    having count(*) = 1
),
patients as (
select
    clarity_patient.patient_key as patient_key,
    'SAFETRACE~' || patient.patient_id as integration_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    patient.patient_id as blood_bank_patient_id,
    clarity_patient.mrn,
    coalesce(patient.abo_cd, 'NA') as abo_type,
    coalesce(patient.rh_cd, 'NA') as rh_factor,
    current_timestamp as create_date,
    'SAFETRACE' as create_source,
    current_timestamp as update_date,
    'SAFETRACE' as update_source
from {{source('safetrace_ods', 'safetrace_patient')}} as patient
inner join {{source('safetrace_ods', 'safetrace_patient_medical_record')}} as patient_medical_record
    on patient_medical_record.patient_id = patient.patient_id
inner join clarity_patient
    on clarity_patient.mrn = patient_medical_record.medical_recno
left join unique_patients
    on unique_patients.medical_recno = patient_medical_record.medical_recno
where patient_medical_record.current_mrn = 'Y' -- current MRN
    -- only pull (1) active or (2) unique, non-merged merged patients
    and (patient.patient_status_cd = 'A'
        or (patient.patient_status_cd != 'M'
            and unique_patients.medical_recno is not null))
---
union all
---
select
    0,
    'NA',
    0,
    'NA',
    'NA',
    'NA',
    'NA',
    current_timestamp,
    'DEFAULT',
    current_timestamp,
    'DEFAULT'
)
select
    patients.patient_key,
    patients.integration_id,
    patients.hash_value,
    patients.blood_bank_patient_id,
    patients.mrn,
    patients.abo_type,
    patients.rh_factor,
    patients.create_date,
    patients.create_source,
    patients.update_date,
    patients.update_source
from
    patients
