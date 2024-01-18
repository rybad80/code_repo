{{ config(meta = {
    'critical': true
}) }}

with budget_master_date as (
    select distinct
        master_date.f_yyyy,
        master_date.f_mm,
        master_date.fy_yyyy,
        date_trunc('month', master_date.full_dt) as monthyear,
        day(last_day(date_trunc('month', master_date.full_dt))) as month_day_cnt
    from
        {{source('cdw', 'master_date')}} as master_date
    where
        master_date.dt_key >= 20180701
),

cost_center_description as (
    select
        cost_center.cost_cntr_cd,
        cost_center.cost_cntr_nm
    from
        {{source('cdw', 'cost_center')}} as cost_center
    where
        cost_center.create_by = 'WORKDAY'
)

select
    budget_master_date.monthyear as post_date_month,
    master_statistic.stat_cd as statistic_code,
    case when master_statistic.stat_cd = 14
        then 'Observation Patient Days'
        else master_statistic.stat_nm
    end as metric_name,
    company.comp_id as company_id,
    company.comp_nm as company_name,
    cost_center.cost_cntr_cd as cost_center_code,
    cost_center_description.cost_cntr_nm as cost_center_name,
    'CS_051' as cost_center_site_id,
    'Horizon' as cost_center_site_name,
    sum(budget.month_amt) as metric_budget_value,
    budget_master_date.month_day_cnt
from
    {{source('cdw', 'budget')}} as budget
    left join budget_master_date
        on budget_master_date.f_yyyy = substr(budget.budget_data_set_id, 7, 4)
        and budget_master_date.f_mm = budget.fiscal_month_num
    left join {{source('cdw', 'company')}} as company
        on company.comp_key = budget.comp_key
    left join  {{source('cdw', 'cost_center')}} as cost_center
        on cost_center.cost_cntr_key = budget.cost_cntr_key
    left join cost_center_description
        on cost_center_description.cost_cntr_cd = cost_center.cost_cntr_cd
    left join   {{source('workday', 'workday_cost_center_site')}} as workday_cost_center_site
        on workday_cost_center_site.cost_cntr_site_id = cost_center.rpt_grp_1
    left join  {{source('cdw', 'master_statistic')}} as master_statistic
        on master_statistic.stat_cd = cast(substr(budget.budget_sub_acct, 7, 2) as numeric(2, 0))
where
    to_number(substr(budget.budget_data_set_id, 7, 4), '9999') >= 2019
    and budget.cost_cntr_key not in (0, -1)
    and cost_center.cost_cntr_cd in ('14025', '14050')
group by
    budget_master_date.monthyear,
    company.comp_id,
    company.comp_nm,
    cost_center.gl_comp,
    cost_center.cost_cntr_cd,
    master_statistic.stat_cd,
    master_statistic.stat_nm,
    cost_center_description.cost_cntr_nm,
    cost_center.rpt_grp_1,
    workday_cost_center_site.cost_cntr_site_nm,
    budget_master_date.month_day_cnt,
    master_statistic.stat_cd
union
select
    budget_master_date.monthyear as post_date_month,
    master_statistic.stat_cd as statistic_code,
    case when master_statistic.stat_cd = 14
        then 'Observation Patient Days'
        else master_statistic.stat_nm
    end as metric_name,
    company.comp_id as company_id,
    company.comp_nm as company_name,
    cost_center.gl_comp as cost_center_code, --cost_center_ledger_id,
    cost_center_description.cost_cntr_nm as cost_center_name,
    cost_center.rpt_grp_1 as cost_center_site_id,
    workday_cost_center_site.cost_cntr_site_nm as cost_center_site_name,
    sum(budget.month_amt) as metric_budget_value,
    budget_master_date.month_day_cnt
from
    {{source('cdw', 'budget')}} as budget
    left join budget_master_date
        on budget_master_date.f_yyyy = substr(budget.budget_data_set_id, 7, 4)
        and budget_master_date.f_mm = budget.fiscal_month_num
    left join {{source('cdw', 'company')}} as company
        on company.comp_key = budget.comp_key
    left join  {{source('cdw', 'cost_center')}} as cost_center
        on cost_center.cost_cntr_key = budget.cost_cntr_key
    left join cost_center_description
        on cost_center_description.cost_cntr_cd = cost_center.gl_comp
    left join  {{source('workday', 'workday_cost_center_site')}} as workday_cost_center_site
        on workday_cost_center_site.cost_cntr_site_id = cost_center.rpt_grp_1
    left join  {{source('cdw', 'master_statistic')}} as master_statistic
        on master_statistic.stat_cd = cast(substr(budget.budget_sub_acct, 7, 2) as numeric(2, 0))
where
    to_number(substr(budget.budget_data_set_id, 7, 4), '9999') >= 2019
    and budget.cost_cntr_key not in (0, -1)
    and cost_center.gl_comp is not null
group by
    budget_master_date.monthyear,
    company.comp_id,
    company.comp_nm,
    cost_center.gl_comp,
    cost_center.cost_cntr_cd,
    master_statistic.stat_cd,
    master_statistic.stat_nm,
    cost_center_description.cost_cntr_nm,
    cost_center.rpt_grp_1,
    workday_cost_center_site.cost_cntr_site_nm,
    budget_master_date.month_day_cnt,
    master_statistic.stat_cd
union
select
    budget_master_date.monthyear as post_date_month,
    master_statistic.stat_cd as statistic_code,
    case when master_statistic.stat_cd = 14
        then 'Observation Patient Days'
        else master_statistic.stat_nm
    end as metric_name,
    company.comp_id as company_id,
    company.comp_nm as company_name,
    department.gl_prefix as cost_center_code, --cost_center_ledger_id,
    cost_center.cost_cntr_nm as cost_center_name,
    department.rpt_grp_3 as cost_center_site_id,
    workday_cost_center_site.cost_cntr_site_nm as cost_center_site_name,
    sum(budget.month_amt) as metric_budget_value,
    budget_master_date.month_day_cnt
from
    {{source('cdw', 'budget')}} as budget
    left join budget_master_date
        on budget_master_date.f_yyyy = substr(budget.budget_data_set_id, 7, 4)
        and budget_master_date.f_mm = budget.fiscal_month_num
    left join {{source('cdw', 'company')}} as company
        on company.comp_key = budget.comp_key
    left join {{source('cdw', 'department')}} as department
        on department.dept_key = budget.dept_key
    left join  {{source('cdw', 'cost_center')}} as cost_center
        on cost_center.cost_cntr_cd = department.gl_prefix
        and cost_center.create_by = 'WORKDAY'
    left join  {{source('workday', 'workday_cost_center_site')}} as workday_cost_center_site
        on workday_cost_center_site.cost_cntr_site_id = department.rpt_grp_3
    left join  {{source('cdw', 'master_statistic')}} as master_statistic
        on master_statistic.stat_cd = cast(substr(budget.budget_sub_acct, 7, 2) as numeric(2, 0))
where
    to_number(substr(budget.budget_data_set_id, 7, 4), '9999') >= 2019
    and budget.dept_key not in (0, -1)
group by
    budget_master_date.monthyear,
    company.comp_id,
    company.comp_nm,
    department.gl_prefix,
    master_statistic.stat_cd,
    master_statistic.stat_nm,
    cost_center.cost_cntr_nm,
    department.rpt_grp_3,
    workday_cost_center_site.cost_cntr_site_nm,
    budget_master_date.month_day_cnt,
    master_statistic.stat_cd
