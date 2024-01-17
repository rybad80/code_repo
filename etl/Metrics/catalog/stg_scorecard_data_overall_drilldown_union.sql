{% set cols = 
    "domain,
    metric_name,
    cy,
    c_mm,
    prev_cy,
    fy,
    f_mm,
    prev_fy,
    visual_month,
    drill_down_one,
    drill_down_two,
    metric_id,
    metric_type,
    num_agg,
    denom_agg,
    source_agg"
%}
select {{ cols }} from {{ ref('stg_scorecard_data_overall_cy') }}
union all
select {{ cols }} from {{ ref('stg_scorecard_data_drill_down_one_cy') }}
union all
select {{ cols }} from {{ ref('stg_scorecard_data_drill_down_two_cy') }}
union all
select {{ cols }} from {{ ref('stg_scorecard_data_overall_fy') }}
union all
select {{ cols }} from {{ ref('stg_scorecard_data_drill_down_one_fy') }}
union all
select {{ cols }} from {{ ref('stg_scorecard_data_drill_down_two_fy') }}
