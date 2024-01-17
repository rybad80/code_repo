{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_flex_p7_selected
capture cost center pay periods where both the flex target and productive FTEs exist
*/
with cc_fte as (
    select
        metric_dt_key,
        cost_center_id,
        sum(actual_prdctv_fte) as productive_fte_total
    from
        {{ ref('stg_nursing_flex_p6_productive_fte') }}
    where
        metric_abbreviation = 'FlexFTEcc'
    group by
        metric_dt_key,
        cost_center_id
),

cc_flex_target as (
    select
        metric_dt_key,
        cost_center_id,
        sum(numerator) as cc_pp_flex_target_total
    from
        {{ ref('stg_nursing_flex_p5_component') }}
    where
        metric_abbreviation = 'FlexTrgtCc'
    group by
        metric_dt_key,
        cost_center_id
)

select
    cc_flex_target.metric_dt_key,
    cc_flex_target.cost_center_id,
    cc_flex_target.cc_pp_flex_target_total,
    cc_fte.productive_fte_total
from
    cc_flex_target
      inner join cc_fte
        on cc_flex_target.metric_dt_key = cc_fte.metric_dt_key
        and cc_flex_target.cost_center_id = cc_fte.cost_center_id
where
    cc_flex_target.cc_pp_flex_target_total > 0
    and cc_fte.productive_fte_total > 0
