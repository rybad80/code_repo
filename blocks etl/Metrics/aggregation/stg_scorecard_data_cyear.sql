select
    domain,
    metric_name,
    metric_type,
    num_agg,
    denom_agg,
    drill_down_one,
    drill_down_two,
    visual_month,
    cy,
    c_mm,
    prev_cy,
    {{- calculate_metric_value('num_agg', 'denom_agg') -}} as metric_value_cytd,
    metric_id
from
    {{ ref('stg_scorecard_data_overall_drilldown_union') }}
where
    source_agg='cyear'
group by
    domain,
    metric_name,
    metric_type,
    num_agg,
    denom_agg,
    drill_down_one,
    drill_down_two,
    visual_month,
    cy,
    c_mm,
    prev_cy,
    metric_id
