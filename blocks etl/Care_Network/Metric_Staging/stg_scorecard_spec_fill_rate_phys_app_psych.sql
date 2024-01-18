select
    domain,
    'Fill Rate (Physician/APP/Psych Visit)' as metric_name,
    primary_key,
    specialty_name as drill_down_one,
    department_center as drill_down_two,
    metric_date,
    num,
    denom,
    num_calculation,
    denom_calculation,
    metric_type,
    desired_direction,
    'spec_fill_rate_phys_app_psych' as metric_id
from
    {{ref('stg_metrics_fill_rate')}}
where
    specialty_care_slot_ind = 1
    and ancillary_services_ind = 0
