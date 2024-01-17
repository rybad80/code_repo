{{ config(meta = {
    'critical': true
}) }}

select
    stg_finance_month_cost_center_budget.post_date_month,
    master_date.full_dt as post_date,
    stg_finance_month_cost_center_budget.statistic_code,
    stg_finance_month_cost_center_budget.metric_name,
    stg_finance_month_cost_center_budget.company_id,
    stg_finance_month_cost_center_budget.company_name,
    stg_finance_month_cost_center_budget.cost_center_code,
    stg_finance_month_cost_center_budget.cost_center_name,
    stg_finance_month_cost_center_budget.cost_center_site_id,
    stg_finance_month_cost_center_budget.cost_center_site_name,
    round(
        stg_finance_month_cost_center_budget.metric_budget_value
        / stg_finance_month_cost_center_budget.month_day_cnt, 4
    ) as metric_budget_value
from
    {{ref('stg_finance_month_cost_center_budget')}} as stg_finance_month_cost_center_budget
inner join
    {{source('cdw', 'master_date')}} as master_date
        on stg_finance_month_cost_center_budget.post_date_month = date_trunc('month', master_date.full_dt)
