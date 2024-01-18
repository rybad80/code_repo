with cyear_drill_down_one as (
    select
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.cy,
        stage_drill_down_one.c_mm,
        stage_drill_down_one.prev_cy,
        NULL as fy,
        NULL as f_mm,
        NULL as prev_fy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        'drill_down_one' as drill_down_two,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stg_scorecard_data_drill_down_one.c_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_drill_down_one') }} as stage_drill_down_one
        left join {{ ref('stg_scorecard_data_drill_down_one') }} as stg_scorecard_data_drill_down_one
            on stage_drill_down_one.metric_id = stg_scorecard_data_drill_down_one.metric_id
                and stage_drill_down_one.cy = stg_scorecard_data_drill_down_one.cy
                and stage_drill_down_one.drill_down_one = stg_scorecard_data_drill_down_one.drill_down_one
                and stg_scorecard_data_drill_down_one.c_mm <= stage_drill_down_one.c_mm
    group by
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.cy,
        stage_drill_down_one.c_mm,
        stage_drill_down_one.prev_cy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        stg_scorecard_data_drill_down_one.num_calculation,
        stg_scorecard_data_drill_down_one.denom_calculation
),
prev_cytd_drill_down_one as (
        select
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.cy,
        NULL as c_mm,
        NULL as prev_cy,
        NULL as fy,
        NULL as f_mm,
        NULL as prev_fy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        'drill_down_one' as drill_down_two,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stage_drill_down_one.c_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_drill_down_one') }} as stage_drill_down_one
        left join {{ ref('stg_scorecard_data_drill_down_one') }} as stg_scorecard_data_drill_down_one
            on stage_drill_down_one.metric_id = stg_scorecard_data_drill_down_one.metric_id
                and stage_drill_down_one.prev_cy = stg_scorecard_data_drill_down_one.cy
                and stage_drill_down_one.drill_down_one = stg_scorecard_data_drill_down_one.drill_down_one
                and stg_scorecard_data_drill_down_one.c_day <= stage_drill_down_one.c_day
                and stg_scorecard_data_drill_down_one.c_mm <= stage_drill_down_one.c_mm
    group by
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.cy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        stg_scorecard_data_drill_down_one.num_calculation,
        stg_scorecard_data_drill_down_one.denom_calculation
)
select *,'cyear' as source_agg  from cyear_drill_down_one
union all
select *,'prev_cytd' as source_agg  from prev_cytd_drill_down_one
