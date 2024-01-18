with cyear_overall as(
    select
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.cy,
        stage_overall.c_mm,
        stage_overall.prev_cy,
        NULL as fy,
        NULL as f_mm,
        NULL as prev_fy,
        stage_overall.visual_month,
        'overall' as drill_down_one,
        'overall' as drill_down_two,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stg_scorecard_data_overall.c_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_overall') }} as stage_overall
        left join {{ ref('stg_scorecard_data_overall') }} as stg_scorecard_data_overall
            on stage_overall.metric_id = stg_scorecard_data_overall.metric_id
                and stage_overall.cy = stg_scorecard_data_overall.cy
                and stg_scorecard_data_overall.c_mm <= stage_overall.c_mm
    group by
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.cy,
        stage_overall.c_mm,
        stage_overall.prev_cy,
        stage_overall.visual_month,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        stg_scorecard_data_overall.num_calculation,
        stg_scorecard_data_overall.denom_calculation
),
prev_cytd_overall as (
        select
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.cy,
        NULL as c_mm,
        NULL as prev_cy,
        NULL as fy,
        NULL as f_mm,
        NULL as prev_fy,
        stage_overall.visual_month,
        'overall' as drill_down_one,
        'overall' as drill_down_two,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stage_overall.c_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_overall') }} as stage_overall
        left join {{ ref('stg_scorecard_data_overall') }} as stg_scorecard_data_overall
            on stage_overall.metric_id = stg_scorecard_data_overall.metric_id
                and stage_overall.prev_cy = stg_scorecard_data_overall.cy
                and stg_scorecard_data_overall.c_day <= stage_overall.c_day
                and stg_scorecard_data_overall.c_mm <= stage_overall.c_mm
    group by
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.cy,
        stage_overall.visual_month,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        stg_scorecard_data_overall.num_calculation,
        stg_scorecard_data_overall.denom_calculation
)
select *,'cyear' as source_agg  from cyear_overall
union all
select *,'prev_cytd' as source_agg  from prev_cytd_overall
