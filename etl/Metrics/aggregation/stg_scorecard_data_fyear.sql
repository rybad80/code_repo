select
    domain,
    metric_name,
    metric_type,
    num_agg,
    denom_agg,
    drill_down_one,
    drill_down_two,
    visual_month,
    fy,
    f_mm,
    prev_fy,
    {{- calculate_metric_value('num_agg', 'denom_agg') -}} as metric_value_fytd,
    metric_id
from
   {{ ref('stg_scorecard_data_overall_drilldown_union') }}
where
    source_agg='fyear'
group by
    domain,
    metric_name,
    metric_type,
    num_agg,
    denom_agg,
    drill_down_one,
    drill_down_two,
    visual_month,
    fy,
    f_mm,
    prev_fy,
    metric_id
