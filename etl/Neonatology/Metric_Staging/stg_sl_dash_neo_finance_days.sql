with days_in_month as (
    select
        full_dt,
        day_of_mm,
        date_trunc('month', full_dt) as visual_month
    from
        {{ source('cdw', 'master_date') }}
    where
        last_day_month_ind = 1 and full_dt <= current_date
        or (full_dt = current_date - 1)
),

finance_metrics as (
    select
        finance_patient_days_actual.pat_key,
        finance_patient_days_actual.visit_key,
        finance_patient_days_actual.post_date,
        days_in_month.day_of_mm,
        finance_patient_days_actual.cost_center_name,
        finance_patient_days_actual.cost_center_site_name,
        finance_patient_days_actual.patient_day_type,
        {{
            dbt_utils.surrogate_key([
                'finance_patient_days_actual.visit_key',
                'finance_patient_days_actual.post_date',
                'finance_patient_days_actual.cost_center_ledger_id',
                'finance_patient_days_actual.patient_day_type'
            ])
        }} as primary_key,
        sum(finance_patient_days_actual.patient_days_actual) as patient_days,
        /* allows consistent adc calc throughout pipeline */
        cast(days_in_month.day_of_mm as decimal(6, 4))
        / count(distinct primary_key)
            over (partition by date_trunc('month', finance_patient_days_actual.post_date))
        as average_daily_census_denom
    from
        {{ ref('finance_patient_days_actual') }} as finance_patient_days_actual
        inner join days_in_month
            on finance_patient_days_actual.post_date_month = days_in_month.visual_month
        inner join {{ ref('lookup_cost_center_service_line') }} as lookup_cost_center_service_line
            on
                finance_patient_days_actual.cost_center_ledger_id
                = lookup_cost_center_service_line.cost_center_gl_id
    where
        lower(lookup_cost_center_service_line.service_line) = 'neonatology'
    group by
        finance_patient_days_actual.pat_key,
        finance_patient_days_actual.visit_key,
        finance_patient_days_actual.post_date,
        finance_patient_days_actual.post_date_month,
        finance_patient_days_actual.cost_center_name,
        finance_patient_days_actual.cost_center_site_name,
        finance_patient_days_actual.patient_day_type,
        finance_patient_days_actual.cost_center_ledger_id,
        days_in_month.day_of_mm
)

select
    finance_metrics.primary_key,
    finance_metrics.post_date,
    finance_metrics.patient_days,
    stg_sl_dash_neo_visits.cohort_group,
    stg_sl_dash_neo_visits.admission_source,
    finance_metrics.average_daily_census_denom
from
    finance_metrics
    inner join {{ ref('stg_sl_dash_neo_visits') }} as stg_sl_dash_neo_visits
        on stg_sl_dash_neo_visits.visit_key = finance_metrics.visit_key
