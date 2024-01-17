select
    post_date_month,
    cost_center_name,
    sum(patient_days_target) as patient_days_target
from
    {{ ref('stg_cancer_center_patient_days')}}
group by
    post_date_month,
    cost_center_name
