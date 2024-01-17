{{ config(meta = {
    'critical': true
}) }}

/* timereport_daily_all
initial capture of Kronos daily time aggregates by worker, cost center, job code,
and paycode that can support downstream TDL such as for Nursing and other areas
needing employee time totals as recorded in Kronos
two key indicators for early branching logic:
-- hppd_job_grouper (Staff RN, APRNs, and patient care unlicensed assistive personnel thus excludes LPNs)
-- productive_direct_ind
*/

with select_hppd_job_codes as (
    select
        job_code,
        job_title_display,
        'PatCareRN' as hppd_job_group_id
    from
        {{ ref('job_code_profile') }}
    where
        rn_job_ind = 1
        and nccs_direct_care_staff_ind = 1

    union all

    select
        job_code,
        job_title_display,
        'UAP' as hppd_job_group_id
    from
        {{ ref('job_code_profile') }}
    where
        lower(nursing_category) like '%unlicensed assistive personnel%'
        and nccs_direct_care_staff_ind = 1
),

paycode_grouper as ( /* time not REGULAR and not OVERTIME */
    select
        attribute_type,
        wf_kronos_code,
        attribute_value as time_subset,
        productive_type,
        case productive_type
            when 'direct'
            then 1 else 0
        end as productive_direct_ind,
        case productive_type
            when 'indirect'
            then 1 else 0
        end as productive_indirect_ind,
        case productive_type
            when 'non'
            then 1 else 0
        end as non_productive_ind
    from
        {{ ref('lookup_paycode_attribute') }}
    where
        productive_type in (
            'direct',
            'indirect',
            'non'
        )
)

select
    person.personnum as worker_id,
    stg_date_nursing_pay_period.pp_end_dt_key,
    detail.adjapplydtm as metric_date,
    to_char(detail.adjapplydtm, 'yyyymmdd') as metric_dt_key,
    substring(labor_account.laborlev1nm, 1, 3) as company_id,
    substring(labor_account.laborlev1nm, 4, 5) as cost_center_id,
    'CS_' || substring(labor_account.laborlev1nm, 10, 3) as cost_center_site_id,
    job_org.orgpathtxt as timereport_org_path,
    labor_account.laborlev5nm as job_code,
    select_hppd_job_codes.hppd_job_group_id,
    pay_code.name as timereport_paycode,
    coalesce(paycode_grouper.productive_direct_ind, 0) as productive_direct_ind,
    case when paycode_grouper.productive_direct_ind = 1
        and paycode_grouper.time_subset = 'callback'
        then 1 else 0
    end as callback_ind,
    case
        when pay_code.productvtytypeid = 1
        then 1 else 0
    end as dart_time_worked_ind,
    paycode_grouper.time_subset,
    coalesce(paycode_grouper.productive_indirect_ind, 0) as productive_indirect_ind,
    coalesce(paycode_grouper.non_productive_ind, 0) as non_productive_ind,
    sum((detail.durationsecsqty::numeric(15, 4)) / 3600) as worker_daily_total,
    sum(timesheet.moneyeditamt) as money_daily_total,
    stg_date_nursing_pay_period.date_fiscal_year,
    stg_date_nursing_pay_period.pp_fiscal_year,
    detail.wfctotalid as wfctotal_id,
    detail.paycodeid as timereport_paycode_id,
    pay_code.productvtytypeid as productivity_type_id,
    detail.timesheetitemid as timesheet_item_id,
    detail.laboracctid as labor_accounting_id,
    detail.wfcjoborgid  as job_organization_id,
    stg_date_nursing_pay_period.nccs_platform_window_ind
from       {{ source('kronos_ods', 'kronos_wfctotal') }} as detail
inner join {{ ref('stg_date_nursing_pay_period') }} as stg_date_nursing_pay_period
        on detail.adjapplydtm = stg_date_nursing_pay_period.full_date
inner join {{ source('kronos_ods', 'wtkemployee') }} as kronos_emp
        on detail.employeeid = kronos_emp.wtkemployeeid
inner join {{ source('kronos_ods', 'kronos_person') }} as person
        on kronos_emp.personid = person.personid
inner join {{ source('kronos_ods', 'paycode') }} as pay_code
        on detail.paycodeid = pay_code.paycodeid
inner join {{ source('kronos_ods', 'laboracct') }} as labor_account
        on detail.laboracctid = labor_account.laboracctid
left join {{ source('kronos_ods', 'wfcjoborg') }} as job_org
        on detail.wfcjoborgid = job_org.wfcjoborgid
left join {{ source('kronos_ods', 'kronos_timesheetitem') }} as timesheet
        on detail.timesheetitemid = timesheet.timesheetitemid
left join paycode_grouper
        on pay_code.name = paycode_grouper.wf_kronos_code
left join select_hppd_job_codes
        on labor_account.laborlev5nm = select_hppd_job_codes.job_code
where
    detail.adjapplydtm >= '12-15-2019' /* Kronos upgrade go live w Workday */
    --and stg_date_nursing_pay_period.nccs_platform_window_ind = 1
group by
    person.personnum,
    stg_date_nursing_pay_period.pp_end_dt_key,
    detail.adjapplydtm,
    labor_account.laborlev1nm, /* Lvl1: company cost_center site */
    job_org.orgpathtxt,
    labor_account.laborlev5nm, /* Lvl 5: job */
    select_hppd_job_codes.hppd_job_group_id,
    stg_date_nursing_pay_period.date_fiscal_year,
    stg_date_nursing_pay_period.pp_fiscal_year,
    pay_code.name,
    paycode_grouper.productive_direct_ind,
    paycode_grouper.productive_indirect_ind,
    paycode_grouper.time_subset,
    paycode_grouper.non_productive_ind,
    detail.wfctotalid,
    detail.paycodeid,
    pay_code.productvtytypeid,
    detail.timesheetitemid,
    detail.laboracctid,
    detail.wfcjoborgid,
    stg_date_nursing_pay_period.nccs_platform_window_ind
