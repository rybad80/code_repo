select
    'operational' as domain, --noqa: L029
    'pfex' as subdomain,
    'Online Scheduling (Physician/APP/Psych Visits)' as metric_name,
    stg_scorecard_online_scheduling.metric_date,
    stg_scorecard_online_scheduling.primary_key,
    initcap(stg_scorecard_online_scheduling.drill_down_one) as drill_down_one,
    initcap(stg_scorecard_online_scheduling.drill_down_two) as drill_down_two,
    stg_scorecard_online_scheduling.phys_app_psych_online_scheduled_ind as num,
    stg_scorecard_online_scheduling.visit_key as denom,
    'sum' as num_calculation,
    'count' as denom_calculation,
    'percentage' as metric_type,
    'up' as desired_direction,
    'spec_online_sched_phys_app_psych' as metric_id
from
    {{ref('stg_scorecard_online_scheduling')}} as stg_scorecard_online_scheduling
where
    stg_scorecard_online_scheduling.physician_app_psych_visit_ind = 1
    and stg_scorecard_online_scheduling.ancillary_services_ind = 0
