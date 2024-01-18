select
    domain,
    subdomain,
    metric_name || ' ' || '(Ancillary Services)' as metric_name,
    primary_key,
    metric_date,
    num,
    denom,
    num_calculation,
    denom_calculation,
    metric_type,
    desired_direction,
    'spec_late_cancel_no_show_ancillary_services' as metric_id,
    specialty_name as drill_down_one,
    department_center as drill_down_two
from
    {{ref('stg_metrics_no_show_late_cancel')}}
where
    ancillary_services_ind = 1
