{{ config(meta = {
    'critical': true
}) }}

with stg_charge as (
	select
	pat_key,
	service_date,
	post_date,
	charge_amount,
	care_setting,
	ip_ed_ind,
	revenue_location,
    payor_name,
	department_name,
	department_center,
	cost_center_name,
	cost_center_site,
	gps_ind,
	source
	from
	{{ref('stg_charge_hb')}}
	union all
		select
	pat_key,
	service_date,
	post_date,
	charge_amount,
	care_setting,
	ip_ed_ind,
	revenue_location,
    payor_name,
	department_name,
	department_center,
	cost_center_name,
	cost_center_site,
	gps_ind,
	source
	from
	{{ref('stg_charge_pb')}}
)
select
	pat_key,
	service_date,
	cost_center_name,
	care_setting,
	ip_ed_ind,
	revenue_location,
    payor_name,
	department_name,
	department_center,
	cost_center_site,
	source,
	gps_ind,
	sum(charge_amount) as charge_amount,
	min(post_date) as first_post_date
from
	stg_charge
group by
	pat_key,
	service_date,
    cost_center_name,
	care_setting,
	ip_ed_ind,
	revenue_location,
	source,
	gps_ind,
    payor_name,
	department_name,
	department_center,
	cost_center_site
