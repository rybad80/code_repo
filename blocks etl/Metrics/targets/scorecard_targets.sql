select
    *
from
    {{ ref('stg_scorecard_targets_overall') }}

union all

select
    *
from
    {{ ref('stg_scorecard_targets_drill_down_one') }}

union all

select
    *
from
    {{ ref('stg_scorecard_targets_drill_down_two') }}
