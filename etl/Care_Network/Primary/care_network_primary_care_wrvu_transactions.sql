{{
	config(
		materialized = 'incremental',
		unique_key = 'tdl_id',
		meta = {
			'critical': true
		}
	)
}}


with rvu_calculation as (
	select
		stg_care_network_operations.tdl_id,
		stg_care_network_operations.post_date,
		stg_care_network_operations.orig_service_date,
		stg_care_network_operations.period,
		stg_care_network_operations.effective_rvu_year,
		stg_care_network_operations.cpt_code,
        stg_care_network_operations.modifier_one,
        stg_care_network_operations.modifier_two,
        stg_care_network_operations.modifier_three,
        stg_care_network_operations.modifier_four,
		stg_care_network_operations.procedure_quantity,
        stg_care_network_operations.amount as tdl_total_amount,
        stg_care_network_operations.work_rvu,
        stg_care_network_operations.effective_rvu_work,
        stg_care_network_operations.calculated_rvu,
		stg_care_network_operations.tdl_extract_date
	from {{ ref('stg_care_network_operations') }} as stg_care_network_operations
	where stg_care_network_operations.detail_type in (1, 10)
	and stg_care_network_operations.post_date between '2021-07-01' and (current_date - 1)
),

--region attribute wRVU transactions to real worker IDs
rvu_attribution as (
	select
		stg_care_network_operations.tdl_id,
		stg_care_network_operations.csn,
		stg_care_network_operations.location_id,
		service_worker.provider_id as tdl_service_provider_id,
		service_worker.provider_full_name as tdl_service_provider_name,
		service_worker.prov_key as service_provider_key,
		service_worker.provider_type as service_provider_type,
		service_worker.provider_worker_id as service_provider_worker_id,
		billing_worker.provider_id as tdl_billing_provider_id,
		billing_worker.provider_full_name as tdl_billing_provider_name,
		billing_worker.prov_key as billing_provider_key,
		billing_worker.provider_worker_id as billing_provider_worker_id,
		-- assign wRVU values from generic or missing providers to actual employees
		case
			when tdl_service_provider_id in (
				'6076', '2003599', '684983', '611948', '670174'
				)
			then tdl_billing_provider_id
			else tdl_service_provider_id
		end as rvu_provider_id,
		case
			when tdl_service_provider_id in (
				'6076', '2003599', '684983', '611948', '670174'
				)
			then billing_worker.prov_key
			else service_worker.prov_key
		end as rvu_provider_key,
		case
			when tdl_service_provider_id in (
				'6076', '2003599', '684983', '611948', '670174'
				)
			then billing_provider_worker_id
			else service_provider_worker_id
		end as rvu_provider_worker_id,
		stg_care_network_operations.department_id
	from {{ ref('stg_care_network_operations') }} as stg_care_network_operations
	inner join rvu_calculation
		on rvu_calculation.tdl_id = stg_care_network_operations.tdl_id
	left join {{ ref('stg_care_network_distinct_worker') }} as service_worker
		on service_worker.provider_id = stg_care_network_operations.servicing_prov_id
		and lower(service_worker.provider_type) in ('physician', 'nurse practitioner')
	left join {{ ref('stg_care_network_distinct_worker') }} as billing_worker
		on billing_worker.provider_id = stg_care_network_operations.billing_prov_id
)

select
	rvu_calculation.tdl_id,
	rvu_attribution.csn,
	rvu_attribution.rvu_provider_worker_id,
	rvu_attribution.tdl_service_provider_name as rvu_provider_full_name,
	worker.provider_last_name as rvu_provider_last_name,
	worker.provider_first_name as rvu_provider_first_name,
	worker.provider_middle_initial as rvu_provider_middle_name,
	rvu_attribution.billing_provider_worker_id,
	rvu_attribution.tdl_billing_provider_id as billing_provider_id,
	rvu_attribution.rvu_provider_id as rvu_provider_id,
	rvu_attribution.service_provider_type as rvu_provider_type,
	rvu_attribution.location_id,
	lookup_care_network_department_cost_center_sites.cost_center_id,
	lookup_care_network_department_cost_center_sites.cost_center_site_id,
	lookup_care_network_department_cost_center_sites.cost_center_site_name,
	lookup_care_network_department_cost_center_sites.cost_center_description,
	cast(rvu_calculation.orig_service_date as date) as orig_service_date,
	rvu_calculation.post_date,
	rvu_calculation.period,
	rvu_calculation.cpt_code,
	rvu_calculation.modifier_one,
	rvu_calculation.modifier_two,
	rvu_calculation.modifier_three,
	rvu_calculation.modifier_four,
	procedure.proc_nm as procedure_name,
	rvu_calculation.procedure_quantity,
	rvu_calculation.effective_rvu_year,
	rvu_calculation.calculated_rvu,
	rvu_calculation.tdl_total_amount,
	stg_encounter.mrn,
	stg_encounter.patient_class,
	stg_encounter.sex,
	stg_encounter.patient_address_zip_code,
	stg_encounter.dob,
	stg_encounter.patient_name,
	stg_encounter.age_years,
	rvu_attribution.department_id,
	dim_department.department_name,
	rvu_calculation.tdl_extract_date
from rvu_attribution
inner join rvu_calculation
	on rvu_calculation.tdl_id = rvu_attribution.tdl_id
left join {{ ref('stg_care_network_distinct_worker') }} as worker
	on worker.provider_worker_id = rvu_attribution.rvu_provider_worker_id
left join {{ source('cdw', 'procedure') }} as procedure
	on rvu_calculation.cpt_code = procedure.proc_cd
left join {{ ref('lookup_care_network_department_cost_center_sites') }}
	as lookup_care_network_department_cost_center_sites
	on lookup_care_network_department_cost_center_sites.department_id = rvu_attribution.department_id
left join {{ ref('dim_department') }} as dim_department
	on dim_department.department_id = rvu_attribution.department_id
left join {{ ref('stg_encounter') }} as stg_encounter
	on rvu_attribution.csn = stg_encounter.csn
where lookup_care_network_department_cost_center_sites.department_id not in (
	'89296012',
	'82',
	'10100117',
	'37'
)
and {{ limit_dates_for_dev(ref_date = 'rvu_calculation.post_date') }}
{% if is_incremental() %}
	and DATE(
		rvu_calculation.tdl_extract_date
	) >= (select max(date(tdl_extract_date) ) from {{ this }})
{% endif %}
