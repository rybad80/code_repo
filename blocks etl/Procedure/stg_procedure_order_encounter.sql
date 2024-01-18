{{ config(materialized='table', dist='proc_ord_key') }}

select
    stg_procedure_order_clinical_all.*,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.pat_id,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.department_key,
    stg_encounter.encounter_key
from {{ref('stg_procedure_order_clinical_all')}} as stg_procedure_order_clinical_all
inner join {{ref('stg_encounter')}} as stg_encounter
    on stg_encounter.visit_key = stg_procedure_order_clinical_all.visit_key
inner join {{ref('stg_patient')}} as stg_patient
    on stg_patient.pat_key = stg_procedure_order_clinical_all.pat_key
