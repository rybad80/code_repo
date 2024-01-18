select
    finance_patient_days_daily_cost_center.post_date_month,
    finance_patient_days_daily_cost_center.post_date,
    finance_patient_days_daily_cost_center.cost_center_name,
    finance_patient_days_daily_cost_center.cost_center_site_name,
    finance_patient_days_daily_cost_center.patient_day_type,
    {{
        dbt_utils.surrogate_key([
            'finance_patient_days_daily_cost_center.post_date_month',
            'finance_patient_days_daily_cost_center.cost_center_ledger_id',
            'finance_patient_days_daily_cost_center.patient_day_type'
        ])
    }} as primary_key,
    sum(finance_patient_days_daily_cost_center.patient_days_actual) as patient_days,
    sum(finance_patient_days_daily_cost_center.patient_days_budget) as patient_days_target,
    case when lower(cost_center_name) = 'cardiac care unit'
        then 'CCU'
        when lower(cost_center_name) = 'cicu'
        then 'CICU'
        end as drill_down,
    'cardiac_unit_pat_days' as metric_id
from
    {{ ref('finance_patient_days_daily_cost_center') }} as finance_patient_days_daily_cost_center
    inner join {{ ref('lookup_cost_center_service_line') }} as lookup_cost_center_service_line
        on
            finance_patient_days_daily_cost_center.cost_center_ledger_id
            = lookup_cost_center_service_line.cost_center_gl_id
where
    lower(lookup_cost_center_service_line.service_line) = 'cardiac center'
group by
    finance_patient_days_daily_cost_center.post_date_month,
    finance_patient_days_daily_cost_center.post_date,
    finance_patient_days_daily_cost_center.cost_center_name,
    finance_patient_days_daily_cost_center.cost_center_site_name,
    finance_patient_days_daily_cost_center.patient_day_type,
    finance_patient_days_daily_cost_center.cost_center_ledger_id
