select
    'operational' as domain, --noqa: L029
    'Specialty Care Encounters (Ancillary Servies)' as metric_name,
    stg_scorecard_spec_encounter.metric_date,
    stg_scorecard_spec_encounter.primary_key,
    stg_scorecard_spec_encounter.drill_down_one,
    stg_scorecard_spec_encounter.drill_down_two,
    stg_scorecard_spec_encounter.visit_key as num,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as desired_direction,
    'spec_enc_ancillary_services' as metric_id
from
    {{ref('stg_scorecard_spec_encounter')}} as stg_scorecard_spec_encounter
where
    stg_scorecard_spec_encounter.ancillary_services_ind = 1
