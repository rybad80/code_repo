{{ config(
  enabled=false
) }}
/* stg_nccs_kronos_hours_selections retiring after the Nursing - Perf part one - twleve PRs in 2023
this is replaced by the stg_nursing_extrapay... and stg_nccs_saftety_obs... tables
*/
with stg_nursing_cost_center as (
    select cost_center_id,
    count(*)
    from {{ref('lookup_nursing_cost_center')}}
    group by 1
),

gather_hours as (
select
    lookup_paycode_category.paycode_hours_category as pay_cd_grouper,
    case when lookup_paycode_category.paycode_hours_category = 'inducement'
       then 1 else 0
           end as extra_pay_ind,
    employee.legal_reporting_nm as worker_nm,
    employee.emp_key as worker_id,
	detail.time_work_dt,
	substring(labor_account.timereport_labor_acct_lvl_1_nm, 4, 5) as cost_center_id,
	substring(labor_account.timereport_labor_acct_lvl_1_nm, 1, 3) as company,
	'CS_' || substring(labor_account.timereport_labor_acct_lvl_1_nm, 10, 3) as cost_center_site_id,
    labor_account.timereport_labor_acct_lvl_1_desc as worked_cost_center_nm,
    labor_account.timereport_labor_acct_lvl_5_nm as job_code,
    labor_account.timereport_labor_acct_lvl_5_desc as job_title,
    pay_code.timereport_pay_cd_nm as timereport_pay_cd_nm,
    job_org.timereport_job_nm,
    labor_account.timereport_labor_acct_lvl_1_nm as company_cost_center_and_site,
    job_org.timereport_org_path,
    case when job_org.timereport_org_path like '%/Safety Obs'
        then 1 else 0
            end as orgpath_safety_obs_ind,
    case when job_org.timereport_org_path like '%/MOO%'
        then 1 else 0
            end as orgpath_meal_out_of_room_ind,
    case when labor_account.timereport_labor_acct_lvl_5_desc like 'Psychiatric Tech%'
        then 1 else 0
            end as ptech_ind,
    case when (labor_account.timereport_labor_acct_lvl_5_desc in
       ('Nursing Extern-Co-Op I',
       'Nursing Extern-Co-Op II',
       'Nursing Extern-Co-Op III')
        and (job_org.timereport_org_path like '%/Safety Obs'
        or job_org.timereport_org_path like '%/MOO%'))
        then 1 else 0
            end as co_op_safety_obs_ind,
    case when (labor_account.timereport_labor_acct_lvl_5_desc = 'Nursing Tech')
            and (job_org.timereport_org_path like '%/Safety Obs' or job_org.timereport_org_path like '%/MOO%')
        then 1 else 0
            end as ntech_safety_obs_ind,
    case when labor_account.timereport_labor_acct_lvl_5_desc = 'Sitter'
        then 1 else 0
            end as sitter_ind,
    case when (labor_account.timereport_labor_acct_lvl_5_desc in
        ('Emergency Dept Tech I',
        'Emergency Dept Tech',
        'Alt Nursing Tech - $18.58',
        'Unit Based Medication Tech')
        and (job_org.timereport_org_path like '%/Safety Obs'
        or job_org.timereport_org_path like '%/MOO%'))
        then 1 else 0
            end as other_tech_safety_obs_ind,
    case when labor_account.timereport_labor_acct_lvl_5_desc = 'Acute Care Technician'
        then 1 else 0
            end as act_ind,
    case when labor_account.timereport_labor_acct_lvl_5_desc = 'Sr Nursing Aide'
        then 1 else 0
        end as sna_ind,
	case when (orgpath_safety_obs_ind = 1 or orgpath_meal_out_of_room_ind = 1
        or ptech_ind = 1 or co_op_safety_obs_ind = 1 or ntech_safety_obs_ind = 1
            or sitter_ind = 1 or other_tech_safety_obs_ind = 1) and pay_cd_grouper = 'reg_ot'
		then 1 else 0
		end as safety_obs_record_ind,
    sum(detail.time_worked_hours) as hours_worked_sum
from {{source('cdw','timereport_detail')}} as detail
inner join {{source('cdw','dim_timereport_person')}} as person
    on person.dim_timereport_person_key = detail.dim_timereport_person_key
inner join {{source('cdw','employee')}} as employee
    on employee.emp_key = person.emp_key
inner join {{source('cdw','dim_timereport_pay_code')}} as pay_code
    on pay_code.dim_timereport_pay_cd_key = detail.dim_timereport_pay_cd_key
inner join {{source('cdw','dim_timereport_labor_account')}} as labor_account
    on labor_account.dim_timereport_labor_acct_key = detail.dim_timereport_labor_acct_key
inner join {{source('cdw','dim_timereport_job_org')}} as job_org
    on job_org.dim_timereport_job_org_key = detail.dim_timereport_job_org_key
left join  {{source('cdw','timereport_timesheet')}} as timesheet
    on detail.timereport_timesheet_key = timesheet.timereport_timesheet_key
inner join {{ref('lookup_paycode_group_category')}} as lookup_paycode_category
    on lower(pay_code.timereport_pay_cd_nm) = lower(lookup_paycode_category.kronos_paycode_group)
inner join stg_nursing_cost_center
    on substring(labor_account.timereport_labor_acct_lvl_1_nm, 4, 5) = stg_nursing_cost_center.cost_center_id
where
    detail.time_work_dt >= '12-15-2019' and detail.time_work_dt <= current_date
group by
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
)

select * from gather_hours
where (safety_obs_record_ind = 1)
or (pay_cd_grouper not in ('reg_ot', 'inducement')
and (act_ind = 1 or sna_ind = 1))
or (pay_cd_grouper = 'inducement')
