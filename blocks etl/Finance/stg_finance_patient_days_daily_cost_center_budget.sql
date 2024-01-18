{{ config(meta = {
    'critical': true
}) }}

with daily_count as (
select distinct
        stg_finance_patient_days_month_cost_center_budget.post_date_month,
        stg_finance_patient_days_month_cost_center_budget.cost_center_ledger_id,
        stg_finance_patient_days_month_cost_center_budget.cost_center_name,
        stg_finance_patient_days_month_cost_center_budget.cost_center_site_id,
        stg_finance_patient_days_month_cost_center_budget.cost_center_site_name,
        stg_finance_patient_days_month_cost_center_budget.patient_day_type,
        round(
                stg_finance_patient_days_month_cost_center_budget.patient_days_budget
                / stg_finance_patient_days_month_cost_center_budget.month_day_cnt, 2
        ) as patient_days_budget
from
        {{ref('stg_finance_patient_days_month_cost_center_budget')}} as stg_finance_patient_days_month_cost_center_budget --noqa: L016
)

select
    daily_count.post_date_month,
    master_date.full_dt as post_date,
    daily_count.cost_center_ledger_id,
    daily_count.cost_center_name,
    daily_count.cost_center_site_id,
    daily_count.cost_center_site_name,
    daily_count.patient_day_type,
    daily_count.patient_days_budget
from
    daily_count
    inner join {{source('cdw','master_date')}} as master_date
        on daily_count.post_date_month = date_trunc('month', master_date.full_dt)
