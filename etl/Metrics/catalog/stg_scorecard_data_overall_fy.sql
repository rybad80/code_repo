with fyear_overall as (
    select
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.fy,
        stage_overall.f_mm,
        stage_overall.prev_fy,
        NULL as cy,
        NULL as c_mm,
        NULL as prev_cy,
        stage_overall.visual_month,
        'overall' as drill_down_one,
        'overall' as drill_down_two,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stg_scorecard_data_overall.f_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_overall') }} as stage_overall
        left join {{ ref('stg_scorecard_data_overall') }} as stg_scorecard_data_overall
            on stage_overall.metric_id = stg_scorecard_data_overall.metric_id
                and stage_overall.fy = stg_scorecard_data_overall.fy
                and stg_scorecard_data_overall.f_mm <= stage_overall.f_mm
    group by
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.fy,
        stage_overall.f_mm,
        stage_overall.prev_fy,
        stage_overall.visual_month,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        stg_scorecard_data_overall.num_calculation,
        stg_scorecard_data_overall.denom_calculation

),
prev_fytd_overall as (
    select
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.fy,
        NULL as f_mm,
        NULL as prev_fy,
        NULL as cy,
        NULL as c_mm,
        NULL as prev_cy,
        stage_overall.visual_month,
        'overall' as drill_down_one,
        'overall' as drill_down_two,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        {{- numerator_aggregation('num_calculation') -}} as num_agg,
        {{- denominator_aggregation('denom_calculation', 'stage_overall.f_day') -}} as denom_agg
    from
        {{ ref('stg_scorecard_data_stage_overall') }} as stage_overall
        left join {{ ref('stg_scorecard_data_overall') }} as stg_scorecard_data_overall
            on stage_overall.metric_id = stg_scorecard_data_overall.metric_id
                and stage_overall.prev_fy = stg_scorecard_data_overall.fy
                and stg_scorecard_data_overall.f_day <= stage_overall.f_day
                and stg_scorecard_data_overall.f_mm <= stage_overall.f_mm
    group by
        stage_overall.domain,
        stage_overall.metric_name,
        stage_overall.fy,
        stage_overall.visual_month,
        stage_overall.metric_id,
        stg_scorecard_data_overall.metric_type,
        stg_scorecard_data_overall.num_calculation,
        stg_scorecard_data_overall.denom_calculation
)
select *,'fyear' as source_agg  from fyear_overall
union all
select *,'prev_fytd'  as source_agg from prev_fytd_overall
