{{ config(meta = {
    'critical': true
}) }}

select
	pat_key,
	to_date(chrg_svc_dt_key, 'yyyymmdd') as service_date,
	to_date(chrg_post_dt_key, 'yyyymmdd') as post_date,
	chrg_amt as charge_amount,
	case
		when lower(place_of_service.pos_type) = 'inpatient hospital'
			then 'Inpatient/Observation'
		when lower(place_of_service.pos_type) = 'urgent care facility' or upper(loc_nm) = 'URGENT CARE NETWORKS RL'
			then 'Urgent Care'
		when (lower(place_of_service.pos_type)  = 'emergency room - hospital'
			or lower(place_of_service.pos_nm) like '%emergency%'
			or lower(place_of_service.pos_nm) like '%/ed%'
			or upper(dept_nm) = 'PB MAIN EMERGENCY DEPT'
			or upper(dept_nm) like '%EMERGENCY%')
			then 'Emergency'
		when (lower(pos_nm) like '%primary care%'
			or lower(pos_nm) like '%prim care%'
			or upper(dept_nm) = 'MKT 3550 CN CHOP CAMP'
			or (lower(pos_nm) like 'care network%' and lower(pos_nm) != 'care network community rl')
			or upper(dept_nm) = 'WOOD GLOBAL PAT SVCS')
			then 'Primary Care'
		when (lower(pos_nm) like '%scc%'
            or (lower(pos_nm) = 'chop/outpatient' and upper(dept_nm) not like '%EMERGENCY%')
            or upper(dept_nm) = 'MAIN CARDIOLOGY CLINIC'
            or (lower(pos_nm) = 'children''s hospital of philadelphia rl' --noqa: PRS,L048
			and upper(dept_nm) not in (
				'12 NORTHWEST', '5 EAST', '7 SOUTH TOWER', '8 SOUTH TOWER', '9 SOUTH TOWER', 'INP ADOLESCENT'
			)
			)
            or lower(pos_nm) like 'bgr/%'
            or lower(pos_nm) like 'virtua%/op'
            or (lower(pos_nm) = 'chop/telehealth other than patient''s home' --noqa: PRS,L048
			and upper(dept_nm) = 'PB MAIN INTRV RADIOLGY')
            )
			then 'Specialty Care'
		when loc.loc_id in (1012, 1022, 1013, 1023, 1016)
			then 'Specialty Care'
		else 'Other'
		end as care_setting,
	0 as ip_ed_ind,
	loc.loc_nm as revenue_location,
    payor.payor_nm as payor_name,
	department.dept_nm as department_name,
	department.dept_cntr as department_center,
	cost_center.cost_cntr_nm as cost_center_name,
	cost_center_site.cost_center_site_name as cost_center_site,
	case
		when upper(dept_nm) = 'WOOD GLOBAL PAT SVCS'
		then 1
		else 0
		end as gps_ind,
	'pb attribution' as source
from
	{{source('cdw', 'fact_reimbursement')}} as fact_reimbursement
left join
	{{source('cdw', 'department')}} as department
	on fact_reimbursement.dept_key = department.dept_key
left join
	{{source('cdw', 'place_of_service')}} as place_of_service
	on fact_reimbursement.pos_key = place_of_service.pos_key
left join
    {{source('cdw', 'payor')}} as payor
    on fact_reimbursement.orig_payor_key = payor.payor_key
left join
	{{source('cdw', 'location')}} as loc
	on fact_reimbursement.loc_key = loc.loc_key
left join
	{{source('cdw', 'cost_center')}} as cost_center
	on cost_center.cost_cntr_id = department.gl_prefix
    and upper(cost_center.create_by) = 'WORKDAY'
left join
	{{source('workday_ods', 'cost_center_site')}} as cost_center_site
    on cost_center_site.cost_center_site_id = department.rpt_grp_3
where
	pat_key > 0
	and void_user_id is null
	and tran_type_key = 1
