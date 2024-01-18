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
)

select
    budget_master_date.monthyear as post_date_month,
    cost_center.gl_comp as cost_center_ledger_id,
    cost_center.cost_cntr_nm as cost_center_name,
    cost_center.rpt_grp_1 as cost_center_site_id,
    workday_cost_center_site.cost_cntr_site_nm as cost_center_site_name,
    case     when budget.budget_sub_acct = 'ST010032-0000'
            then 'IP Patient Days'
            when budget.budget_sub_acct = 'ST020014-0000'
            then 'Observation Patient Days'
            end as patient_day_type,
    sum(budget.month_amt) as patient_days_budget,
    budget_master_date.month_day_cnt
from
    {{source('cdw', 'budget')}} as budget
    left join budget_master_date
        on budget_master_date.f_yyyy = substr(budget.budget_data_set_id, 7, 4)
            and budget_master_date.f_mm = budget.fiscal_month_num
    left join {{source('cdw','cost_center')}} as cost_center
        on cost_center.cost_cntr_key = budget.cost_cntr_key
    left join {{source('workday', 'workday_cost_center_site')}} as workday_cost_center_site
        on workday_cost_center_site.cost_cntr_site_id = cost_center.rpt_grp_1
    left join {{source('cdw','master_statistic')}} as master_statistic
        on master_statistic.stat_cd = cast(substr(budget.budget_sub_acct, 7, 2) as numeric(2, 0))
where
    to_number(substr(budget.budget_data_set_id, 7, 4), '9999') >= 2019
    and budget.budget_sub_acct in ('ST010032-0000', 'ST020014-0000')
    and budget.cost_cntr_key != -1
group by
    budget_master_date.monthyear,
    cost_center.gl_comp,
    cost_center.cost_cntr_nm,
    cost_center.rpt_grp_1,
    workday_cost_center_site.cost_cntr_site_nm,
    budget.budget_sub_acct,
    budget_master_date.month_day_cnt
