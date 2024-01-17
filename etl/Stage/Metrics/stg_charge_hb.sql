{{ config(meta = {
    'critical': true
}) }}

select
	hospital_account.pat_key,
	to_date(fact_transaction_hb.svc_dt_key, 'yyyymmdd') as service_date,
	to_date(fact_transaction_hb.post_dt_key, 'yyyymmdd') as post_date,
	fact_transaction_hb.chrg_amt as charge_amount,
	case
		when lower(hospital_class.dict_nm) = 'emergency'
			then 'Emergency'
		when lower(hospital_class.dict_nm) in ('outpatient', 'recurring outpatient', 'day surgery')
			then 'Specialty Care'
		when lower(hospital_class.dict_nm) in (	'observation', 'inpatient', 'admit after surgery-obs',
												'admit after surgery-ip', 'newborn', 'admit after surgery',
												'admit before surgery', 'ip deceased organ donor')
			then 'Inpatient/Observation'
		else 'Other'
		end as care_setting,
	case
		when care_setting = 'Inpatient/Observation' and cost_center.cost_cntr_id in (109, 1012)
			then 1
			else 0
			end as ip_ed_ind,
	loc.loc_nm as revenue_location,
	payor.payor_nm as payor_name,
	dept.dept_nm as department_name,
	dept.dept_cntr as department_center,
	cost_center.cost_cntr_nm as cost_center_name,
	cost_center_site.cost_center_site_name as cost_center_site,
	0 as gps_ind,
	'hb attribution' as source
from
	{{source('cdw', 'fact_transaction_hb')}} as fact_transaction_hb
inner join
	{{source('cdw', 'hospital_account')}} as hospital_account
	on fact_transaction_hb.hsp_acct_key = hospital_account.hsp_acct_key
left join
	{{source('cdw', 'cdw_dictionary')}} as cdw_dictionary
	on fact_transaction_hb.dict_acct_class_key = cdw_dictionary.dict_key
left join
    {{source('cdw', 'visit')}} as visit
    on fact_transaction_hb.visit_key = visit.visit_key
left join
    {{source('cdw', 'payor')}} as payor
    on visit.payor_key = payor.payor_key
left join
	{{source('cdw', 'cdw_dictionary')}} as hospital_class
	on fact_transaction_hb.dict_acct_class_key = hospital_class.dict_key
left join
	{{source('cdw', 'location')}} as loc
	on loc.loc_key = fact_transaction_hb.trans_loc_key
left join
	{{source('cdw', 'department')}} as dept
	on fact_transaction_hb.dept_key = dept.dept_key
left join
    {{source('cdw', 'cost_center')}} as cost_center
    on cost_center.cost_cntr_key = fact_transaction_hb.cost_cntr_key
left join
    {{source('workday_ods', 'cost_center_site')}} as cost_center_site
	on cost_center_site.cost_center_site_id = cost_center.rpt_grp_1
where
	hospital_account.pat_key > 0
	and fact_transaction_hb.trans_type_key = 8961
	and fact_transaction_hb.svc_dt_key > 0
