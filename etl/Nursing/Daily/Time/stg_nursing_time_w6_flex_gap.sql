{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_w6_flex_gap
Pulling the actual Productive total FTE which is the Direct & Indirect totals
(total time minus the Time off) and subtracting it from the patient days or other
volume-adjusted target provides each unit's Productive Flex gap for the pay period
Note:  For cost centers that do not "flex" with patient volume, the target (FlexTrgtCc)
is just the sum of the fixed budgets for the month the PP falls in (from flex_p2 table)
*/
with cc_pp_selected as ( /* to determine a productive flex varaince */
    select
        metric_dt_key as pp_for_flex,
        cost_center_id as cc_id_for_flex
    from
        {{ ref('stg_nursing_flex_p7_selected') }}
),

flex_target_component as (
    select
        stg_nursing_flex_p5_component.metric_abbreviation,
        stg_nursing_flex_p5_component.metric_dt_key,
        stg_nursing_flex_p5_component.cost_center_id,
        stg_nursing_flex_p5_component.metric_grouper,
        stg_nursing_flex_p5_component.job_group_id,
        stg_nursing_flex_p5_component.numerator as component_value
    from
        /* only for cost centers with a unit of service for the PP or all fixed roles */
        cc_pp_selected
        inner join {{ ref('stg_nursing_flex_p5_component') }} as stg_nursing_flex_p5_component
            on cc_pp_selected.pp_for_flex =  stg_nursing_flex_p5_component.metric_dt_key
            and cc_pp_selected.cc_id_for_flex = stg_nursing_flex_p5_component.cost_center_id
    where
        stg_nursing_flex_p5_component.metric_abbreviation in (
            'FlexBdgtFixed',
            'FlexCcVwhuosPlnd',
            'FlexTrgtCcVrbl',
            'FlexJgVwhuosPlnd')
),

flex_target as (
    select
        stg_nursing_flex_p5_component.metric_abbreviation,
        stg_nursing_flex_p5_component.metric_dt_key,
        stg_nursing_flex_p5_component.cost_center_id,
        stg_nursing_flex_p5_component.metric_grouper,
        stg_nursing_flex_p5_component.job_group_id,
        stg_nursing_flex_p5_component.numerator as flex_target_cc_fte
    from
        cc_pp_selected
        inner join {{ ref('stg_nursing_flex_p5_component') }} as stg_nursing_flex_p5_component
            on cc_pp_selected.pp_for_flex =  stg_nursing_flex_p5_component.metric_dt_key
            and cc_pp_selected.cc_id_for_flex = stg_nursing_flex_p5_component.cost_center_id
    where
        stg_nursing_flex_p5_component.metric_abbreviation in (
            'FlexTrgtCc',
            'FlexTrgtJr')
),

actual_productive_fte as ( /* at the CC level & at job rollup level if have it */
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        metric_grouper,
        job_group_id,
        actual_prdctv_fte
    from
        {{ ref('stg_nursing_flex_p6_productive_fte') }}
    where
        actual_prdctv_fte != 0
        /* take the AmbulatoryRN FTE totals only when present */
),

actual_fte_for_gap_calc as ( /* limit to only ones with a Flex target for the cc */
    select
        prdctv_fte.metric_abbreviation,
        prdctv_fte.metric_dt_key,
        prdctv_fte.cost_center_id,
        prdctv_fte.metric_grouper,
        prdctv_fte.job_group_id,
        prdctv_fte.actual_prdctv_fte
    from
        cc_pp_selected
        inner join actual_productive_fte as prdctv_fte
            on cc_pp_selected.pp_for_flex =  prdctv_fte.metric_dt_key
            and cc_pp_selected.cc_id_for_flex = prdctv_fte.cost_center_id
),

flex_gap_calc as (
    select
        case
        when coalesce(flex_target.metric_abbreviation, prdctv_fte.metric_abbreviation) in (
            'FlexTrgtJr',
            'FlexFTEjr'
        )
        then 'FlexGapJr'
        else 'FlexGap'
        end as metric_abbreviation,
        coalesce(prdctv_fte.metric_dt_key, flex_target.metric_dt_key) as metric_dt_key,
        coalesce(prdctv_fte.cost_center_id, flex_target.cost_center_id) as cost_center_id,
        flex_target.metric_grouper,
        coalesce(prdctv_fte.job_group_id, flex_target.job_group_id) as job_group_id,
        coalesce(flex_target.flex_target_cc_fte, 0)
        - coalesce(prdctv_fte.actual_prdctv_fte, 0) as numerator,
        flex_target.flex_target_cc_fte as prdctvty_index_numerator,
        prdctv_fte.actual_prdctv_fte as prdctvty_index_denominator,
        case
            when coalesce(prdctv_fte.actual_prdctv_fte, 0) = 0
            then 0 else case
                when coalesce(flex_target.flex_target_cc_fte, 0) = 0
                then 0 else 1
                end
            end as prdctvty_index_calc_ind
    from flex_target
    full outer join actual_fte_for_gap_calc as prdctv_fte /* full outer needed because sometimes budget is missing
        and other times the FTE is missing but there still was a budget (converted to a flex target) */
        on flex_target.metric_dt_key = prdctv_fte.metric_dt_key
        and flex_target.cost_center_id = prdctv_fte.cost_center_id
        and coalesce(flex_target.job_group_id, 'nullJG') = coalesce(prdctv_fte.job_group_id, 'nullJG')
        and ((flex_target.metric_abbreviation = 'FlexTrgtCc'
            and prdctv_fte.metric_abbreviation = 'FlexFTEcc')
        or (flex_target.metric_abbreviation = 'FlexTrgtJr'
            and prdctv_fte.metric_abbreviation = 'FlexFTEjr'))
),

union_set as (
/* the components that assembled make the Productive Flex target for the cc */
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    component_value as numerator
from
    flex_target_component

union all
/* the FlexTrgt rows at cost center and job rollup granularity */
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    flex_target_cc_fte as numerator
from
    flex_target

union all
/* the Flex variances at cost center and job rollup granularity */
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    numerator
from
    flex_gap_calc

union all
/* the Flex productive actuals at cost center and job rollup granularity
    even if no varaince could be calculated */
select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    job_group_id,
    actual_prdctv_fte as numerator
from
    actual_productive_fte
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    union_set

union all
/* the Productivity Index flex target/prdctv actual, ideal 95-101% */
select
    case metric_abbreviation
        when 'FlexGap' then 'PrdctvtyIndexCcORIG'
        else 'PrdctvtyIndexJrORIG'
        end as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    prdctvty_index_numerator as numerator,
    prdctvty_index_denominator as denominator,
    round(prdctvty_index_numerator
        / prdctvty_index_denominator, 5) as row_metric_calculation
from
    flex_gap_calc
where
    prdctvty_index_calc_ind = 1
