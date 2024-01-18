select
    'Oncology Unit Patient Days' as metric_name,
    finance_patient_days_daily_cost_center.post_date_month,
    finance_patient_days_daily_cost_center.post_date,
    finance_patient_days_daily_cost_center.cost_center_name,
    case when finance_patient_days_daily_cost_center.cost_center_name = 'ONCOLOGY BMT - SOUTH'
        then 'Oncology BMT - South'
        when finance_patient_days_daily_cost_center.cost_center_name = 'ONCOLOGY BMT - EAST'
        then 'Oncology BMT - East'
        else finance_patient_days_daily_cost_center.cost_center_name end
        as drill_down,
    finance_patient_days_daily_cost_center.cost_center_site_name,
    finance_patient_days_daily_cost_center.patient_day_type,
    {{
        dbt_utils.surrogate_key([
            'finance_patient_days_daily_cost_center.post_date',
            'finance_patient_days_daily_cost_center.cost_center_ledger_id',
            'finance_patient_days_daily_cost_center.patient_day_type'
        ])
    }} as primary_key,
    finance_patient_days_daily_cost_center.patient_days_actual as patient_days,
    finance_patient_days_daily_cost_center.patient_days_budget as patient_days_target
from
    {{ ref('finance_patient_days_daily_cost_center') }} as finance_patient_days_daily_cost_center
    inner join {{ ref('lookup_cost_center_service_line')}} as lookup_cost_center_service_line
        on
            lookup_cost_center_service_line.cost_center_gl_id
            = finance_patient_days_daily_cost_center.cost_center_ledger_id
where
    lower(service_line) = 'oncology'
