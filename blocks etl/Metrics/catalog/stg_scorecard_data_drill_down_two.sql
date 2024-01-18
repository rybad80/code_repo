select distinct
    {{ dbt_utils.star(from = ref_for_env('stg_scorecard_data')) }}

from
    {{ ref_for_env('stg_scorecard_data') }}
where
    /* properly aggregate harm index */
    metric_name != 'Harm Index'
    or ( metric_name = 'Harm Index' and drill_down_one != 'overall')
