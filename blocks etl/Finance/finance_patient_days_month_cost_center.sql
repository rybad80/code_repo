{{ config(meta = {
    'critical': true
}) }}

with budget_master_date as (
    select distinct
        master_date.f_yyyy,
        master_date.f_mm,
        master_date.fy_yyyy,
        date_trunc('month', master_date.full_dt) as monthyear,
        case when monthyear = date_trunc('month', current_date) --noqa: L028
                and day(current_date) = 1
                then 1
                when monthyear = date_trunc('month', current_date)  --noqa: L028
                then day(current_date) - 1
                else day(last_day(date_trunc('month', master_date.full_dt)))
        end as month_day_cnt_actual,
        day(last_day(date_trunc('month', master_date.full_dt))) as month_day_cnt_budget
    from
        {{source('cdw','master_date')}} as master_date
    where
        master_date.dt_key >= 20180701
),

patient_days as (
    select
        finance_patient_days_daily_cost_center.post_date_month,
        finance_patient_days_daily_cost_center.cost_center_ledger_id,
        finance_patient_days_daily_cost_center.patient_day_type,
        sum(
            finance_patient_days_daily_cost_center.patient_days_budget
        ) as patient_days_budget,
        sum(
            finance_patient_days_daily_cost_center.patient_days_actual
        ) as patient_days_actual,
        sum(
            finance_patient_days_daily_cost_center.patient_days_actual
            ) / max(month_day_cnt_actual) as average_daily_census_actual,
        sum(
            finance_patient_days_daily_cost_center.patient_days_budget
            ) / max(month_day_cnt_budget) as average_daily_census_budget
    from
        {{ref('finance_patient_days_daily_cost_center')}} as finance_patient_days_daily_cost_center
        inner join budget_master_date
            on budget_master_date.monthyear = finance_patient_days_daily_cost_center.post_date_month
    group by
        post_date_month,
        cost_center_ledger_id,
        patient_day_type
)

select
    patient_days.post_date_month,
    patient_days.cost_center_ledger_id,
    stg_cost_center.cost_center_name,
    stg_cost_center.cost_center_site_id,
    stg_cost_center.cost_center_site_name,
    patient_days.patient_day_type,
    patient_days.patient_days_budget,
    patient_days.patient_days_actual,
    patient_days.average_daily_census_actual,
    patient_days.average_daily_census_budget
from
    patient_days
    inner join {{ref('stg_cost_center')}} as stg_cost_center
        on stg_cost_center.cost_center_ledger_id = patient_days.cost_center_ledger_id
