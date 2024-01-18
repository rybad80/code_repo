select
    'operational' as domain, --noqa: L029
    'pfex' as subdomain,
    'Online Scheduling (Ancillary Services)' as metric_name,
    stg_scorecard_online_scheduling.metric_date,
    stg_scorecard_online_scheduling.primary_key,
    initcap(stg_scorecard_online_scheduling.drill_down_one) as drill_down_one,
    initcap(stg_scorecard_online_scheduling.drill_down_two) as drill_down_two,
    stg_scorecard_online_scheduling.online_scheduled_ind as num,
    stg_scorecard_online_scheduling.visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'percentage' as metric_type,
    'up' as desired_direction,
    'spec_online_sched_ancillary_services' as metric_id
from
    {{ref('stg_scorecard_online_scheduling')}} as stg_scorecard_online_scheduling
where
    stg_scorecard_online_scheduling.ancillary_services_ind = 1
