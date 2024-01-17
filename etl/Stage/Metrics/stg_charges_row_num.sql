{{ config(meta = {
    'critical': true
}) }}

select
	pat_key,
	service_date,
	first_post_date,
    cost_center_name,
	care_setting,
	ip_ed_ind,
	revenue_location,
	source,
    payor_name,
	department_name,
	department_center,
	cost_center_site,
	charge_amount,
	row_number() over(partition by pat_key, service_date order by charge_amount desc) as row_num
from
	{{ref('stg_charges_amount')}}
group by
	pat_key,
	service_date,
	first_post_date,
    cost_center_name,
	care_setting,
	ip_ed_ind,
	revenue_location,
	source,
    payor_name,
	department_name,
	department_center,
	cost_center_site,
	charge_amount
