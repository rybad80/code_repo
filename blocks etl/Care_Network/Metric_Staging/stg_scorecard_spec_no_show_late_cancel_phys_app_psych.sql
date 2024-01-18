select
    domain,
    subdomain,
    metric_name || ' ' || '(Physician/APP/Psych Visit)' as metric_name,
    primary_key,
    metric_date,
    num,
    denom,
    num_calculation,
    denom_calculation,
    metric_type,
    desired_direction,
    'spec_late_cancel_no_show_phys_app_psych' as metric_id,
    specialty_name as drill_down_one,
    department_center as drill_down_two
from
    {{ref('stg_metrics_no_show_late_cancel')}}
where
    physician_app_psych_visit_ind = 1
    and ancillary_services_ind = 0
