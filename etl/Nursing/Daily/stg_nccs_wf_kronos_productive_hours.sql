{{ config(meta = {
    'critical': true
}) }}

{{ config(
  enabled=false
) }}
/* stg_nccs_wf_kronos_productive_hours retiring after the
Nursing - Perf part one - twelve PRs in 2023
this is replaced by the timereport_daily_productive_direct in Worker_Time and the
downstream stg_nursing_... tables
*/
with select_hppd_job_codes as (
    select
        job_code,
        job_title_display,
        'RN' as hppd_job_grouper
    from
        {{ ref('job_code_profile') }}
    where
        rn_job_ind = 1
	and nccs_direct_care_staff_ind = 1

    union all

    select
        job_code,
        job_title_display,
        'UAP' as hppd_job_grouper
    from
        {{ ref('job_code_profile') }}
    where
        lower(nursing_category) like '%unlicensed assistive personnel%'
        and nccs_direct_care_staff_ind = 1
)

select
    employee.emp_id as worker_id,
    detail.time_work_dt as metric_date,
    to_char(detail.time_work_dt, 'yyyymmdd') as metric_dt_key,
    substring(labor_account.timereport_labor_acct_lvl_1_nm, 4, 5) as cost_center_id,
    labor_account.timereport_labor_acct_lvl_5_nm as job_code,
    select_hppd_job_codes.hppd_job_grouper,
    stg_nursing_job_code_group.nursing_job_grouper as job_grouper,
    pay_code.timereport_pay_cd_nm as metric_grouper,
    nursing_report_cost_centers.nursing_business_report_select_ind,
    'prdctv_time' as tag,
    sum(detail.time_worked_hours) / 80 as productive_direct_period_full_time_percentage,
    sum(detail.time_worked_hours) as productive_direct_period_hours,
    stg_nursing_job_code_group.provider_or_other_job_group_id,
    stg_nursing_job_code_group.rn_alt_or_other_job_group_id
from       {{ source('cdw','timereport_detail') }} as detail
inner join {{ source('cdw','dim_timereport_person') }} as person
        on person.dim_timereport_person_key = detail.dim_timereport_person_key
inner join {{ source('cdw','employee') }} as employee
        on employee.emp_key = person.emp_key
inner join {{ source('cdw','dim_timereport_pay_code') }} as pay_code
        on pay_code.dim_timereport_pay_cd_key = detail.dim_timereport_pay_cd_key
inner join {{ source('cdw','dim_timereport_labor_account') }} as labor_account
        on labor_account.dim_timereport_labor_acct_key = detail.dim_timereport_labor_acct_key
inner join {{ source('cdw','dim_timereport_job_org') }} as job_org
        on job_org.dim_timereport_job_org_key = detail.dim_timereport_job_org_key
left join  {{ source('cdw','timereport_timesheet') }} as timesheet
        on detail.timereport_timesheet_key = timesheet.timereport_timesheet_key
left join select_hppd_job_codes
        on select_hppd_job_codes.job_code = labor_account.timereport_labor_acct_lvl_5_nm
inner join {{ ref('stg_nursing_job_code_group') }} as stg_nursing_job_code_group
        on stg_nursing_job_code_group.job_code = labor_account.timereport_labor_acct_lvl_5_nm
left join {{ ref('nursing_cost_center_attributes') }} as nursing_report_cost_centers
        on nursing_report_cost_centers.cost_center_id = substring(timereport_labor_acct_lvl_1_nm, 4, 5)
        and nursing_report_cost_centers.nursing_business_report_select_ind = 1
where
    detail.time_work_dt >= '12-15-2019'
    and detail.time_work_dt <= current_date
    and lower(pay_code.timereport_pay_cd_nm) in ('regular', 'overtime')
group by
    employee.emp_id,
    detail.time_work_dt,
    to_char(detail.time_work_dt, 'yyyymmdd'),
    substring(labor_account.timereport_labor_acct_lvl_1_nm, 4, 5),
    labor_account.timereport_labor_acct_lvl_5_nm,
    select_hppd_job_codes.hppd_job_grouper,
    stg_nursing_job_code_group.nursing_job_grouper,
    pay_code.timereport_pay_cd_nm,
    nursing_report_cost_centers.nursing_business_report_select_ind,
    stg_nursing_job_code_group.provider_or_other_job_group_id,
    stg_nursing_job_code_group.rn_alt_or_other_job_group_id
