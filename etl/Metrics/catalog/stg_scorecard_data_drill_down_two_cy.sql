with cyear_drill_down_two as (
    select
        stage_drill_down_two.domain,
        stage_drill_down_two.metric_name,
        stage_drill_down_two.cy,
        stage_drill_down_two.c_mm,
        stage_drill_down_two.prev_cy,
        NULL as fy,
        NULL as f_mm,
        NULL as prev_fy,
        stage_drill_down_two.visual_month,
        stage_drill_down_two.drill_down_one,
        stage_drill_down_two.drill_down_two,
        stage_drill_down_two.metric_id,
        stg_scorecard_data_drill_down_two.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stg_scorecard_data_drill_down_two.c_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_drill_down_two') }} as stage_drill_down_two
        left join {{ ref('stg_scorecard_data_drill_down_two') }} as stg_scorecard_data_drill_down_two
            on stage_drill_down_two.metric_id = stg_scorecard_data_drill_down_two.metric_id
                and stage_drill_down_two.cy = stg_scorecard_data_drill_down_two.cy
                and stage_drill_down_two.drill_down_one = stg_scorecard_data_drill_down_two.drill_down_one
                and stage_drill_down_two.drill_down_two = stg_scorecard_data_drill_down_two.drill_down_two
                and stg_scorecard_data_drill_down_two.c_mm <= stage_drill_down_two.c_mm
    group by
        stage_drill_down_two.domain,
        stage_drill_down_two.metric_name,
        stage_drill_down_two.cy,
        stage_drill_down_two.c_mm,
        stage_drill_down_two.prev_cy,
        stage_drill_down_two.visual_month,
        stage_drill_down_two.drill_down_one,
        stage_drill_down_two.drill_down_two,
        stage_drill_down_two.metric_id,
        stg_scorecard_data_drill_down_two.metric_type,
        stg_scorecard_data_drill_down_two.num_calculation,
        stg_scorecard_data_drill_down_two.denom_calculation
),
prev_cytd_drill_down_two as (
    select
        stage_drill_down_two.domain,
        stage_drill_down_two.metric_name,
        stage_drill_down_two.cy,
        NULL as c_mm,
        NULL as prev_cy,
        NULL as fy,
        NULL as f_mm,
        NULL as prev_fy,
        stage_drill_down_two.visual_month,
        stage_drill_down_two.drill_down_one,
        stage_drill_down_two.drill_down_two,
        stage_drill_down_two.metric_id,
        stg_scorecard_data_drill_down_two.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stage_drill_down_two.c_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_drill_down_two') }} as stage_drill_down_two
        left join {{ ref('stg_scorecard_data_drill_down_two') }} as stg_scorecard_data_drill_down_two
            on stage_drill_down_two.metric_id = stg_scorecard_data_drill_down_two.metric_id
                and stage_drill_down_two.prev_cy = stg_scorecard_data_drill_down_two.cy
                and stage_drill_down_two.drill_down_one = stg_scorecard_data_drill_down_two.drill_down_one
                and stage_drill_down_two.drill_down_two = stg_scorecard_data_drill_down_two.drill_down_two
                and stg_scorecard_data_drill_down_two.c_day <= stage_drill_down_two.c_day
                and stg_scorecard_data_drill_down_two.c_mm <= stage_drill_down_two.c_mm
    group by
        stage_drill_down_two.domain,
        stage_drill_down_two.metric_name,
        stage_drill_down_two.cy,
        stage_drill_down_two.visual_month,
        stage_drill_down_two.drill_down_one,
        stage_drill_down_two.drill_down_two,
        stage_drill_down_two.metric_id,
        stg_scorecard_data_drill_down_two.metric_type,
        stg_scorecard_data_drill_down_two.num_calculation,
        stg_scorecard_data_drill_down_two.denom_calculation
)

select *,'cyear' as source_agg from cyear_drill_down_two
union all
select *,'prev_cytd' as source_agg from prev_cytd_drill_down_two
