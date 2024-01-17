with overall_stage as (
    select
        {{ standard_target_fields() }},
        'overall' as drill_down_one,
        'overall' as drill_down_two,
        /* Used to accurately capture FYTD/CYTD target values for current month of count variables */
        day(last_day(visual_month)) as days_in_month,
        {{ aggregate_target("month_target") }} as overall_month_target,
        /* Used to accurately capture FYTD/CYTD target values for current month of count variables */
        {{ calculate_daily_target() }} as daily_target,
        /* Used to accurately capture FYTD/CYTD target values for current month of count variables */
        {{ count_days_in_visual_month() }} as number_days
    from
        {{ ref('stg_scorecard_targets') }}
    group by
        {{ standard_target_fields() }}
),

cy_targets as (
    select
        {{ standard_target_fields(add_dates=False) }},
        cy, 
        {{ aggregate_target("overall_month_target") }} as annual_cy_target
    from
        overall_stage
    group by
        {{ standard_target_fields(add_dates=False) }},
        cy
),

fy_targets as (
    select
        {{ standard_target_fields(add_dates=False) }},
        fy, 
        {{ aggregate_target("overall_month_target") }} as annual_fy_target
    from
        overall_stage
    group by
        {{ standard_target_fields(add_dates=False) }},
        fy
),

fytd_overall as (
    select
        {{ standard_target_fields(add_dates=False, alias="overall_stage") }},
        overall_stage.visual_month,
        {{
            aggregate_target(
                target_sum="stage2.daily_target*stage2.number_days",
                target_max="overall_stage.overall_month_target",
                alias="overall_stage"
            )
        }} as fytd_overall_target
    from
        overall_stage
        inner join overall_stage as stage2
            on overall_stage.metric_id = stage2.metric_id
                /* allows for FYTD calculations */
                and stage2.fy = overall_stage.fy
                /* allows for FYTD calculations */
                and stage2.f_mm <= overall_stage.f_mm
    where
        date(overall_stage.visual_month) <= current_date
    group by
        {{ standard_target_fields(add_dates=False, alias="overall_stage") }},
        overall_stage.visual_month
),

cytd_overall as (
    select
        {{ standard_target_fields(add_dates=False, alias="overall_stage") }},
        overall_stage.visual_month,
        {{
            aggregate_target(
                target_sum="stage2.daily_target*stage2.number_days",
                target_max="overall_stage.overall_month_target",
                alias="overall_stage"
            )
        }} as cytd_overall_target
    from
        overall_stage
        inner join overall_stage as stage2
            on overall_stage.metric_id = stage2.metric_id
                /* allows for CYTD calculations */
                and stage2.cy = overall_stage.cy
                /* allows for CYTD calculations */
                and stage2.c_mm <= overall_stage.c_mm
    where
        date(overall_stage.visual_month) <= current_date
    group by 
        {{ standard_target_fields(add_dates=False, alias="overall_stage") }},
        overall_stage.visual_month
)

select
    overall_stage.visual_month,
    overall_stage.drill_down_one,
    overall_stage.drill_down_two,
    {{- target_display('overall_stage.overall_month_target') -}} as month_target,
    {{- target_display('cy_targets.annual_cy_target') -}} as annual_cy_target,
    {{- target_display('fy_targets.annual_fy_target') -}} as annual_fy_target,
    {{- target_display('fytd_overall.fytd_overall_target') -}} as fytd_overall_target,
    {{- target_display('cytd_overall.cytd_overall_target') -}} as cytd_overall_target,
    overall_stage.metric_id
from
    overall_stage
    inner join fy_targets
        on overall_stage.metric_id = fy_targets.metric_id
            and overall_stage.fy = fy_targets.fy
    inner join cy_targets
        on overall_stage.metric_id = cy_targets.metric_id
            and overall_stage.cy = cy_targets.cy
    inner join fytd_overall
        on overall_stage.metric_id = fytd_overall.metric_id
            and overall_stage.visual_month = fytd_overall.visual_month
    inner join cytd_overall
        on overall_stage.metric_id = cytd_overall.metric_id
            and overall_stage.visual_month = cytd_overall.visual_month
