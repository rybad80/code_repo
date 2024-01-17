select
    finance_patient_days_month_cost_center.post_date_month,
    sum(finance_patient_days_month_cost_center.patient_days_budget) as patient_days_target,
    sum(finance_patient_days_month_cost_center.average_daily_census_budget) as adc_target
from
    {{ ref('finance_patient_days_month_cost_center') }} as finance_patient_days_month_cost_center
    inner join {{ ref('lookup_cost_center_service_line') }} as lookup_cost_center_service_line
        on finance_patient_days_month_cost_center.cost_center_ledger_id
            = lookup_cost_center_service_line.cost_center_gl_id
where
    lower(lookup_cost_center_service_line.service_line) = 'neonatology'
group by
    finance_patient_days_month_cost_center.post_date_month
