{{
    config(
        materialized = 'view',
        meta = {
        'critical': false
        }
    )
}}
select
    fact_specimen_order.specimen_order_key,
    fact_specimen_order.integration_id,
    fact_specimen_order.procedure_order_id,
    fact_specimen_order.order_status,
    fact_specimen_order.order_class,
    fact_specimen_order.order_priority,
    fact_specimen_order.procedure_id,
    fact_specimen_order.procedure_name,
    fact_specimen_order.research_ind,
    fact_specimen_order.patient_key,
    fact_specimen_order.pat_id,
    stg_patient_ods.mrn,
    fact_specimen_order.order_encounter_key,
    fact_specimen_order.specimen_id,
    fact_specimen_order.specimen_number,
    fact_specimen_order.specimen_collected_ind,
    fact_specimen_order.specimen_collected_datetime,
    fact_specimen_order.specimen_source,
    fact_specimen_order.specimen_type,
    fact_specimen_order.blood_draw_ind,
    fact_specimen_order.specimen_container_count,
    fact_specimen_order.lab_test_key,
    fact_specimen_order.test_id,
    dim_lab_test.test_name,
    fact_specimen_order.department_key,
    fact_specimen_order.department_id,
    dim_department.department_name,
    dim_department.specialty_name,
    dim_department.intended_use_name,
    fact_specimen_order.lab_section_key,
    fact_specimen_order.lab_section_id,
    dim_lab_section.lab_section_name,
    fact_specimen_order.authorizing_provider_id,
    fact_specimen_order.authorizing_provider_name,
    fact_specimen_order.ordering_worker_id,
    fact_specimen_order.ordering_worker_name,
    fact_specimen_order.ordering_worker_job_title,
    fact_specimen_order.collecting_worker_id,
    fact_specimen_order.collecting_worker_name,
    fact_specimen_order.collecting_worker_job_title,
    case
        when lower(fact_specimen_order.collecting_worker_job_title) like '%phlebotomist%'
            or lower(fact_specimen_order.collecting_worker_job_title) like '%clinical lab assistant%'
                then 'Phlebotomist'
        when lower(fact_specimen_order.collecting_worker_job_title) like '%physician%' then 'Physician'
        when lower(fact_specimen_order.collecting_worker_job_title) like '%nurse%'
            or lower(fact_specimen_order.collecting_worker_job_title) like '%nursing%'
            or fact_specimen_order.collecting_worker_job_title like '%LPN%'
            or fact_specimen_order.collecting_worker_job_title like '% RN%'
            or fact_specimen_order.collecting_worker_job_title like '%RN %'
                then 'Nurse'
        when fact_specimen_order.collecting_worker_id = '0' then 'Automated'
        else 'Other'
    end as collecting_worker_role,
    fact_specimen_order.cancellation_reason,
    fact_specimen_order.cancellation_comment,
    fact_specimen_order.cancelling_worker_id,
    fact_specimen_order.cancelling_worker_name,
    fact_specimen_order.received_in_uc_lab_datetime,
    fact_specimen_order.received_in_koph_lab_datetime
from
    {{ref('fact_specimen_order')}} as fact_specimen_order
inner join {{ref('dim_lab_section')}} as dim_lab_section
    on dim_lab_section.lab_section_key = fact_specimen_order.lab_section_key
inner join {{ref('dim_lab_test')}} as dim_lab_test
    on dim_lab_test.lab_test_key = fact_specimen_order.lab_test_key
left join {{ref('dim_department')}} as dim_department
    on dim_department.department_key = fact_specimen_order.department_key
inner join {{ref('stg_patient_ods')}} as stg_patient_ods
    on fact_specimen_order.patient_key = stg_patient_ods.patient_key
