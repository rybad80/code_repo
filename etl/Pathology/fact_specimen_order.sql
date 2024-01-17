{{
  config(
    meta = {
      'critical': false
    }
  )
}}
with specimen_containers as (
	select
		fact_specimen_container.specimen_id,
		count(*) as specimen_container_count
	from {{ref('fact_specimen_container')}} as fact_specimen_container
	where
		fact_specimen_container.initial_container_ind = 1
	group by
		fact_specimen_container.specimen_id
),
order_containers as (
	select
		stg_specimen_order_containers.procedure_order_id,
		min(fact_specimen_container.received_in_uc_lab_datetime) as received_in_uc_lab_datetime,
		min(fact_specimen_container.received_in_koph_lab_datetime) as received_in_koph_lab_datetime
	from {{ref('stg_specimen_order_containers')}} as stg_specimen_order_containers
	inner join {{ref('fact_specimen_container')}} as fact_specimen_container
		on fact_specimen_container.container_id = stg_specimen_order_containers.container_id
	group by
		stg_specimen_order_containers.procedure_order_id
)
select
{{
        dbt_utils.surrogate_key(["'CLARITY'", 'stg_specimen_order.procedure_order_id'])
}} as specimen_order_key,
'CLARITY~' || stg_specimen_order.procedure_order_id as integration_id,
stg_specimen_order.procedure_order_id,
stg_specimen_order.order_status,
stg_specimen_order.order_class,
stg_specimen_order.order_priority,
stg_specimen_order.procedure_id,
stg_specimen_order.procedure_name,
stg_specimen_order.research_ind,
stg_specimen_order.patient_key,
stg_specimen_order.pat_id,
stg_encounter.encounter_key as order_encounter_key,
stg_specimen_order.specimen_id,
stg_specimen_order.specimen_number,
stg_specimen_order.specimen_collected_ind,
stg_specimen_order.specimen_collected_datetime,
stg_specimen_order.specimen_source,
stg_specimen_order.specimen_type,
stg_specimen_order.blood_draw_ind,
coalesce(specimen_containers.specimen_container_count, 0) as specimen_container_count,
stg_specimen_order.lab_test_key,
stg_specimen_order.test_id,
stg_specimen_order.department_key,
stg_specimen_order.department_id,
stg_specimen_order.lab_section_key,
stg_specimen_order.lab_section_id,
stg_specimen_order.authorizing_provider_id,
stg_specimen_order.authorizing_provider_name,
stg_specimen_order.ordering_worker_id,
stg_specimen_order.ordering_worker_name,
stg_specimen_order.ordering_worker_job_title,
stg_specimen_order.cancellation_reason,
stg_specimen_order.cancellation_comment,
stg_specimen_order.cancelling_worker_id,
stg_specimen_order.cancelling_worker_name,
stg_specimen_order.collecting_worker_id,
stg_specimen_order.collecting_worker_name,
stg_specimen_order.collecting_worker_job_title,
order_containers.received_in_uc_lab_datetime,
order_containers.received_in_koph_lab_datetime
from {{ref('stg_specimen_order')}} as stg_specimen_order
left join {{ref('stg_encounter')}} as stg_encounter
	on stg_encounter.csn = stg_specimen_order.pat_enc_csn_id
left join specimen_containers
	on specimen_containers.specimen_id = stg_specimen_order.specimen_id
left join order_containers
	on order_containers.procedure_order_id = stg_specimen_order.procedure_order_id
