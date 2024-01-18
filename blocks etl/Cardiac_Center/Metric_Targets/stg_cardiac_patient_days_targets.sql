select
    post_date_month,
    drill_down,
    metric_id,
    sum(patient_days_target) as patient_days_target
from
    {{ ref('stg_sl_dash_cardiac_patient_days')}}
group by
    post_date_month,
    drill_down,
    metric_id
