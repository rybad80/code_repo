select distinct
	worker.worker_id as provider_worker_id,
	provider.prov_id as provider_id,
	upper(worker.preferred_reporting_name) as provider_full_name,
	upper(worker_ods.preferred_last_name) as provider_last_name,
	upper(worker.preferred_first_name) as provider_first_name,
	case
		when length(trim(regexp_extract(
            provider_full_name, ' .$', 1, 1))) = 1
		then trim(regexp_extract(
            provider_full_name, ' .$', 1, 1))
		else null
	end as provider_middle_initial,
	worker.prov_key as prov_key,
	provider.prov_type as provider_type,
	position_title
from {{ ref('worker') }} as worker
left join {{ ref('lookup_care_network_department_cost_center_sites') }}
	as lookup_care_network_department_cost_center_sites
	on worker.cost_center_id = lookup_care_network_department_cost_center_sites.cost_center_id
left join {{ source('workday_ods', 'worker') }} as worker_ods
	on worker_ods.worker_id = worker.worker_id
left join {{ source('cdw', 'provider') }} as provider
	on worker.prov_key = provider.prov_key
