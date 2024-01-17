{{
  config(
    meta = {
      'critical': true
    }
  )
}}
with lab_results as ( --noqa: PRS,L01
    select
        lookup_bioresponse_lab_component.diagnosis_hierarchy_1,
        lookup_bioresponse_lab_component.diagnosis_hierarchy_2,
        procedure_order_result_clinical.mrn,
        procedure_order_result_clinical.csn,
        procedure_order_result_clinical.procedure_id,
        procedure_order_result_clinical.procedure_order_result_key,
        procedure_order_result_clinical.encounter_date,
        procedure_order_result_clinical.department_key,
        procedure_order_result_clinical.procedure_name,
        procedure_order_result_clinical.placed_date,
        procedure_order_result_clinical.order_specimen_source,
        procedure_order_result_clinical.specimen_taken_date,
        procedure_order_result_clinical.department_name,
        procedure_order_result_clinical.result_date,
        procedure_order_result_clinical.result_component_name,
        procedure_order_result_clinical.result_status,
        procedure_order_result_clinical.result_value,
        case
          when (
              regexp_like(upper(procedure_order_result_clinical.result_value), 'DETECTED|REPORTED')
              and upper(procedure_order_result_clinical.result_value) not like '%NOT%'
          )
           or upper(procedure_order_result_clinical.result_value) like '%POSITIVE%'
          then 1
          else 0
        end as positive_ind
    from
        {{ ref('stg_bioresponse_distinct_lab') }} as lookup_bioresponse_lab_component
        inner join {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
            on lookup_bioresponse_lab_component.procedure_id = procedure_order_result_clinical.procedure_id
            and lookup_bioresponse_lab_component.result_component_id = procedure_order_result_clinical.result_component_id -- noqa: L016
    where
        procedure_order_result_clinical.result_status not in ('Incomplete', 'Preliminary', 'NOT APPLICABLE')
        and (
            procedure_order_result_clinical.test_cancelled_ind = 0
            or procedure_order_result_clinical.test_cancelled_ind is null
        )
        and procedure_order_result_clinical.encounter_date >= {{ var('start_data_date') }}
)

select
    lab_results.diagnosis_hierarchy_1,
    lab_results.diagnosis_hierarchy_2,
    lab_results.mrn,
    lab_results.csn,
    lab_results.procedure_id,
    lab_results.procedure_order_result_key,
    stg_encounter.patient_key,
    stg_encounter.encounter_key,
    lab_results.department_key,
    lab_results.encounter_date,
    stg_encounter.encounter_type,
    lab_results.procedure_name,
    lab_results.placed_date,
    lab_results.order_specimen_source,
    lab_results.specimen_taken_date,
    lab_results.department_name,
    lab_results.result_date,
    lab_results.result_component_name,
    lab_results.result_status,
    lab_results.result_value,
    extract(year from age(lab_results.placed_date, stg_patient.dob)) as age_at_test,
    stg_patient.mailing_zip as patient_mailing_zip,
    dim_department.intended_use_name,
    lab_results.positive_ind
from
    lab_results
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on lab_results.csn = stg_encounter.csn
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_encounter.patient_key = stg_patient.patient_key
    inner join {{ ref('dim_department') }} as dim_department
        on stg_encounter.department_key = dim_department.department_key
