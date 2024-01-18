{{ config(materialized='table', dist='encounter_key') }}

with unionset as (
    select
        encounter_key,
        dx_id
    from
        {{ref('stg_dx_problem_list')}}
    union
    select
        encounter_key,
        dx_id
    from
        {{ref('stg_dx_visit_diagnosis')}}
    union
    select
        encounter_key,
        dx_id
    from
        {{ref('stg_dx_pb_transaction')}}
)

select
    unionset.encounter_key,
    unionset.dx_id as diagnosis_id,
    master_diagnosis.dx_key,
    dim_diagnosis.current_icd9_code as icd9_code,
    dim_diagnosis.current_icd10_code as icd10_code,
    dim_diagnosis.diagnosis_name,
    dim_diagnosis.external_diagnosis_id
from
    unionset
    inner join {{ref('dim_diagnosis')}} as dim_diagnosis
        on dim_diagnosis.diagnosis_id = unionset.dx_id
    left join {{source('cdw','master_diagnosis')}} as master_diagnosis
        on master_diagnosis.dx_id = dim_diagnosis.diagnosis_id
