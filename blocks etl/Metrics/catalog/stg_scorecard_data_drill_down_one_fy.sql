with fyear_drill_down_one as (
        select
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.fy,
        stage_drill_down_one.f_mm,
        stage_drill_down_one.prev_fy,
        NULL as cy,
        NULL as c_mm,
        NULL as prev_cy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        'drill_down_one' as drill_down_two,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stg_scorecard_data_drill_down_one.f_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_drill_down_one') }} as stage_drill_down_one
        left join {{ ref('stg_scorecard_data_drill_down_one') }} as stg_scorecard_data_drill_down_one
            on stage_drill_down_one.metric_id = stg_scorecard_data_drill_down_one.metric_id
                and stage_drill_down_one.fy = stg_scorecard_data_drill_down_one.fy
                and stage_drill_down_one.drill_down_one = stg_scorecard_data_drill_down_one.drill_down_one
                and stg_scorecard_data_drill_down_one.f_mm <= stage_drill_down_one.f_mm
    group by
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.fy,
        stage_drill_down_one.f_mm,
        stage_drill_down_one.prev_fy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        stg_scorecard_data_drill_down_one.num_calculation,
        stg_scorecard_data_drill_down_one.denom_calculation
),
prev_fytd_drill_down_one as (
        select
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.fy,
        NULL as f_mm,
        NULL as prev_fy,
        NULL as cy,
        NULL as c_mm,
        NULL as prev_cy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        'drill_down_one' as drill_down_two,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stage_drill_down_one.f_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_drill_down_one') }} as stage_drill_down_one
        left join {{ ref('stg_scorecard_data_drill_down_one') }} as stg_scorecard_data_drill_down_one
            on stage_drill_down_one.metric_id = stg_scorecard_data_drill_down_one.metric_id
                and stage_drill_down_one.prev_fy = stg_scorecard_data_drill_down_one.fy
                and stage_drill_down_one.drill_down_one = stg_scorecard_data_drill_down_one.drill_down_one
                and stg_scorecard_data_drill_down_one.f_day <= stage_drill_down_one.f_day
                and stg_scorecard_data_drill_down_one.f_mm <= stage_drill_down_one.f_mm
    group by
        stage_drill_down_one.domain,
        stage_drill_down_one.metric_name,
        stage_drill_down_one.fy,
        stage_drill_down_one.visual_month,
        stage_drill_down_one.drill_down_one,
        stage_drill_down_one.metric_id,
        stg_scorecard_data_drill_down_one.metric_type,
        stg_scorecard_data_drill_down_one.num_calculation,
        stg_scorecard_data_drill_down_one.denom_calculation
)
select *,'fyear' as source_agg  from fyear_drill_down_one
union all
select *,'prev_fytd' as source_agg  from prev_fytd_drill_down_one