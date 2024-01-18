select  distinct
    stg_scorecard_data_up_to_date.visual_month,
    stg_scorecard_data_up_to_date.domain,
    stg_scorecard_data_up_to_date.subdomain,
    stg_scorecard_data_up_to_date.metric_name,
    stg_scorecard_data_up_to_date.drill_down_one,
    stg_scorecard_data_up_to_date.drill_down_two,
    stg_scorecard_data_up_to_date.fy_seq_num,
    stg_scorecard_data_up_to_date.fy_month_seq_num,
    stg_scorecard_data_up_to_date.fy,
    stg_scorecard_data_up_to_date.cy,
    stg_scorecard_data_up_to_date.cy_month_seq_num,
    stg_scorecard_data_up_to_date.cy_seq_num,
    stg_scorecard_data_up_to_date.prev_fy,
    stg_scorecard_data_up_to_date.metric_type,
    stg_scorecard_data_up_to_date.desired_direction,
    {{- aggregate_display('stg_scorecard_data_month.metric_value_month_td') -}}  as metric_value_month_td,
    {{- aggregate_display('stg_scorecard_data_fyear.metric_value_fytd') -}} as metric_value_fytd,
    stg_scorecard_data_fyear.num_agg as fy_num_agg,
    stg_scorecard_data_fyear.denom_agg as fy_denom_agg,
    {{- aggregate_display('stg_scorecard_data_prev_fyear.metric_value_prev_fytd') -}} as metric_value_prev_fytd,
    {{- aggregate_display('stg_scorecard_data_cyear.metric_value_cytd') -}} as metric_value_cytd,
    stg_scorecard_data_cyear.num_agg as cy_num_agg,
    stg_scorecard_data_cyear.denom_agg as cy_denom_agg,
    {{- aggregate_display('stg_scorecard_data_prev_cyear.metric_value_prev_cytd') -}} as metric_value_prev_cytd,
    scorecard_targets.fytd_overall_target as target_fytd,
    scorecard_targets.cytd_overall_target as target_cytd,
    scorecard_targets.month_target,
    scorecard_targets.annual_fy_target,
    scorecard_targets.annual_cy_target,
    {{- determine_metric_red_threshold('target_fytd') -}} as red_threshold,
    {{- determine_metric_red_threshold('target_cytd') -}} as cy_red_threshold,
    {{- determine_target_interpretation('stg_scorecard_data_fyear.metric_value_fytd', 'target_fytd') -}} as interpretation_fy_target,
    {{-
        determine_interpretation_status('interpretation_fy_target', 'stg_scorecard_data_fyear.metric_value_fytd', 'red_threshold')
    -}} as interpretation_fy_status,
    {{- determine_target_interpretation('stg_scorecard_data_cyear.metric_value_cytd', 'target_cytd') -}} as interpretation_cy_target,
    {{-
        determine_interpretation_status('interpretation_cy_target', 'stg_scorecard_data_cyear.metric_value_cytd', 'cy_red_threshold')
    -}} as interpretation_cy_status,
    stg_scorecard_data_up_to_date.as_of_date,
    stg_scorecard_data_up_to_date.metric_id
from
    {{ ref('stg_scorecard_data_up_to_date') }} as stg_scorecard_data_up_to_date
    left join {{ ref('stg_scorecard_data_month') }} as stg_scorecard_data_month
        on stg_scorecard_data_up_to_date.visual_month = stg_scorecard_data_month.visual_month
            {{- standard_join_fields('stg_scorecard_data_up_to_date','stg_scorecard_data_month')-}}
    left join {{ ref('stg_scorecard_data_fyear') }} as stg_scorecard_data_fyear
        on stg_scorecard_data_up_to_date.fy = stg_scorecard_data_fyear.fy
            {{- standard_join_fields('stg_scorecard_data_up_to_date','stg_scorecard_data_fyear')-}}
            and stg_scorecard_data_fyear.visual_month = stg_scorecard_data_up_to_date.visual_month
    left join {{ ref('stg_scorecard_data_prev_fytd') }} as stg_scorecard_data_prev_fyear
        on stg_scorecard_data_prev_fyear.visual_month = stg_scorecard_data_up_to_date.visual_month
            {{- standard_join_fields('stg_scorecard_data_up_to_date','stg_scorecard_data_prev_fyear')-}}
        left join {{ ref('stg_scorecard_data_cyear') }} as stg_scorecard_data_cyear
        on stg_scorecard_data_up_to_date.cy = stg_scorecard_data_cyear.cy
            {{- standard_join_fields('stg_scorecard_data_up_to_date','stg_scorecard_data_cyear')-}}
            and stg_scorecard_data_cyear.visual_month = stg_scorecard_data_up_to_date.visual_month
    left join {{ ref('stg_scorecard_data_prev_cytd') }} as stg_scorecard_data_prev_cyear
        on stg_scorecard_data_prev_cyear.visual_month = stg_scorecard_data_up_to_date.visual_month
            {{- standard_join_fields('stg_scorecard_data_up_to_date','stg_scorecard_data_prev_cyear')-}}
    left join {{ ref('scorecard_targets') }} as scorecard_targets
        on stg_scorecard_data_up_to_date.visual_month = scorecard_targets.visual_month
            {{- standard_join_fields('stg_scorecard_data_up_to_date','scorecard_targets')-}}


