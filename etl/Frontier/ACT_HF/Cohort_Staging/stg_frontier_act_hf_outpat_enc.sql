select enc.mrn,
	enc.visit_key,
	enc.specialty_name
from {{ ref('stg_frontier_act_hf_pat_base')}} as pat_base
inner join {{ ref('encounter_specialty_care') }} as enc
	on pat_base.mrn = enc.mrn
	and pat_base.min_hospital_discharge_date <= enc.encounter_date
inner join {{ ref('lookup_frontier_program_departments')}} as lk_department
	on enc.department_id = lk_department.department_id
	and lk_department.program = 'act-hf'
inner join {{ ref('lookup_frontier_program_visit')}} as lk_visit
	on enc.visit_type_id = lk_visit.id
	and lk_department.department_type = lk_visit.category
	and lk_visit.program = 'act-hf'
inner join {{source('cdw','provider')}} as p
	on enc.prov_key = p.prov_key
left join {{ ref('lookup_frontier_program_providers_all')}} as lk_provider
	on p.prov_id =  lk_provider.provider_id
	and lk_provider.program = 'act-hf'
where lk_visit.category != 'cardiology'
	or lk_provider.provider_id is not null
