{{ config(meta = {
    'critical': true
}) }}

with cost_center_dates as (
    select
        master_date.full_dt as post_date,
        stg_cost_center.cost_center_ledger_id
    from
        {{source('cdw','master_date')}} as master_date
        cross join {{ref('stg_cost_center')}} as stg_cost_center
    group by
        master_date.full_dt,
        stg_cost_center.cost_center_ledger_id
),

cost_center_dates_pt_day as (
    select
        cost_center_dates.post_date,
        cost_center_dates.cost_center_ledger_id,
        stg_finance_daily_cost_center_budget.metric_name as patient_day_type
    from
        cost_center_dates
        inner join {{ref('stg_finance_daily_cost_center_budget')}} as stg_finance_daily_cost_center_budget
            on stg_finance_daily_cost_center_budget.post_date = cost_center_dates.post_date
            and stg_finance_daily_cost_center_budget.cost_center_code = cost_center_dates.cost_center_ledger_id
            and stg_finance_daily_cost_center_budget.statistic_code in ('32', '14')
    union
    select
        cost_center_dates.post_date,
        cost_center_dates.cost_center_ledger_id,
        finance_patient_days_actual.patient_day_type
    from
        cost_center_dates
        inner join {{ref('finance_patient_days_actual')}} as finance_patient_days_actual
            on finance_patient_days_actual.post_date = cost_center_dates.post_date
            and finance_patient_days_actual.cost_center_ledger_id = cost_center_dates.cost_center_ledger_id
    where
        finance_patient_days_actual.patient_days_actual is not null
),

actual_agg as (
    select
        finance_patient_days_actual.post_date,
        finance_patient_days_actual.cost_center_ledger_id,
        finance_patient_days_actual.patient_day_type,
        sum(finance_patient_days_actual.patient_days_actual) as patient_days_actual
    from
        {{ref('finance_patient_days_actual')}} as finance_patient_days_actual
    group by
        finance_patient_days_actual.post_date,
        finance_patient_days_actual.cost_center_ledger_id,
        finance_patient_days_actual.patient_day_type
)

select
    date_trunc('month', cost_center_dates_pt_day.post_date) as post_date_month,
    cost_center_dates_pt_day.post_date,
    cost_center_dates_pt_day.cost_center_ledger_id,
    stg_cost_center.cost_center_name,
    stg_cost_center.cost_center_site_id,
    stg_cost_center.cost_center_site_name,
    cost_center_dates_pt_day.patient_day_type,
    stg_finance_daily_cost_center_budget.metric_budget_value
    as patient_days_budget,
    actual_agg.patient_days_actual
from
    cost_center_dates_pt_day
    left join actual_agg
        on actual_agg.post_date = cost_center_dates_pt_day.post_date
        and actual_agg.cost_center_ledger_id = cost_center_dates_pt_day.cost_center_ledger_id
        and actual_agg.patient_day_type = cost_center_dates_pt_day.patient_day_type
    left join {{ref('stg_finance_daily_cost_center_budget')}} as stg_finance_daily_cost_center_budget
        on stg_finance_daily_cost_center_budget.post_date = cost_center_dates_pt_day.post_date
        and stg_finance_daily_cost_center_budget.cost_center_code = cost_center_dates_pt_day.cost_center_ledger_id
        and stg_finance_daily_cost_center_budget.metric_name = cost_center_dates_pt_day.patient_day_type
        and stg_finance_daily_cost_center_budget.statistic_code in (14, 32)
    inner join {{ref('stg_cost_center')}} as stg_cost_center
        on stg_cost_center.cost_center_ledger_id = cost_center_dates_pt_day.cost_center_ledger_id
