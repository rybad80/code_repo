select
	case when month(service_date) > 6 then year(service_date) + 1
	else year(service_date) end as fy,
	mrn,
	patient_name,
	service_date,
	care_setting,
	newborn_ind,
	new_enterprise_patient_ind,
	new_care_setting_patient_ind,
	region_category,
	chop_market,
	zip,
	first_post_date
from
	{{ref('stg_addl_children_served')}}
where
	(date(service_date) >= to_date('2020-01-01', 'yyyy-mm-dd')
	and date(service_date) < current_date)
