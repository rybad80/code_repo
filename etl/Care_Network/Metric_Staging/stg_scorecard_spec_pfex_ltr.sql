select
    domain, --noqa: L029
    subdomain,
    metric_name,
    primary_key,
    specialty_name as drill_down_one,
    department_center as drill_down_two,
    metric_date,
    num,
    num_calculation,
    denom,
    denom_calculation,
    desired_direction,
    metric_type,
    'spec_ltr' as metric_id
from
    {{ref('stg_metrics_pfex_ltr')}}
where
    lower(survey_line_name) = 'specialty care'
