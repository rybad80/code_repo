{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_unit_w1_patient_days
for fiscal year 2020 forward, roll the patient days to
pay period granularity for budgets and actuals
and calculate variance
*/
with
patient_days_by_pp as (
    select
        nursing_pay_period.pp_end_dt_key as metric_dt_key,
        finance_patient_days_daily_cost_center.cost_center_ledger_id as cost_center_id,
        finance_patient_days_daily_cost_center.cost_center_site_id,
        finance_patient_days_daily_cost_center.patient_day_type,
        sum(finance_patient_days_daily_cost_center.patient_days_budget) as pp_patient_days_budget,
        sum(finance_patient_days_daily_cost_center.patient_days_actual) as pp_patient_days_actual
    from {{ ref('finance_patient_days_daily_cost_center') }} as finance_patient_days_daily_cost_center
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on to_char(finance_patient_days_daily_cost_center.post_date, 'yyyymmdd')
            between nursing_pay_period.pp_start_dt_key and nursing_pay_period.pp_end_dt_key
            and nursing_pay_period.prior_pay_period_ind = 1
    where post_date >= '2019-06-30'
    group by
        nursing_pay_period.pp_end_dt_key,
        finance_patient_days_daily_cost_center.cost_center_ledger_id,
        finance_patient_days_daily_cost_center.cost_center_site_id,
        finance_patient_days_daily_cost_center.patient_day_type
),

upcoming_patient_days_budget_by_pp as (
    select
        nursing_pay_period.pp_end_dt_key as metric_dt_key,
        finance_patient_days_daily_cost_center.cost_center_ledger_id as cost_center_id,
        finance_patient_days_daily_cost_center.cost_center_site_id,
        finance_patient_days_daily_cost_center.patient_day_type,
        sum(finance_patient_days_daily_cost_center.patient_days_budget) as future_pp_patient_days_budget
    from {{ ref('finance_patient_days_daily_cost_center') }} as finance_patient_days_daily_cost_center
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on to_char(finance_patient_days_daily_cost_center.post_date, 'yyyymmdd')
            between nursing_pay_period.pp_start_dt_key and nursing_pay_period.pp_end_dt_key
            and nursing_pay_period.nursing_operations_window_ind = 1
            and nursing_pay_period.future_pay_period_ind = 1
    group by
        nursing_pay_period.pp_end_dt_key,
        finance_patient_days_daily_cost_center.cost_center_ledger_id,
        finance_patient_days_daily_cost_center.cost_center_site_id,
        finance_patient_days_daily_cost_center.patient_day_type

),

aggregates as (
    select
        'UpcomingPatDaysBdgt' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        cost_center_site_id,
        'all' as metric_grouper,
        sum(future_pp_patient_days_budget) as numerator
    from upcoming_patient_days_budget_by_pp
    group by
        metric_dt_key,
        cost_center_id,
        cost_center_site_id

    union all

    select
        'PatDaysPPbdgtTot' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        cost_center_site_id,
        'all' as metric_grouper,
        sum(pp_patient_days_budget) as numerator
    from patient_days_by_pp
    group by
        metric_dt_key,
        cost_center_id,
        cost_center_site_id

    union all

    select
        'PatDaysPPactualTot' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        cost_center_site_id,
        'all' as metric_grouper,
        sum(pp_patient_days_actual) as numerator
    from patient_days_by_pp
    group by
        metric_dt_key,
        cost_center_id,
        cost_center_site_id

    union all

    select
        'PatDaysPPvar' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
       cost_center_site_id,
        'all' as metric_grouper,
        sum(pp_patient_days_actual)
        - sum(pp_patient_days_budget) as numerator
    from patient_days_by_pp
    group by
           metric_dt_key,
        cost_center_id,
        cost_center_site_id

    union all

    select
        'PatDaysPPactualType' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        cost_center_site_id,
        'IP patient days' as metric_grouper,
        pp_patient_days_actual as numerator
    from patient_days_by_pp
    where
        lower(patient_day_type) = 'ip patient days'

    union all

    select
        'PatDaysPPactualType' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        cost_center_site_id,
        'observation patient days' as metric_grouper,
        pp_patient_days_actual as numerator
    from patient_days_by_pp
    where
        lower(patient_day_type) = 'observation patient days'
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    cost_center_site_id,
    null::numeric as department_id,
    null as job_code,
    null as job_group_id,
    metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    aggregates
