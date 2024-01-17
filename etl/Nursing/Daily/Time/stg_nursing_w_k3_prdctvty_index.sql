/* stg_nursing_w_k3_prdctvty_index
the productivity index KPI can only be calcuated when a target and actaul FTE is present; this SQL
handles both the job group rollups that apply and the cost center rollup of the index
*/
with
metric_mapping as (
    /* for job rollup granularity of the index */
    select
    'job rollup' as granularity,
    'FlexFTEjr' as fte_metric,
    'FlexTrgtJr' as target_metric,
    'PrdctvtyIndexJr' as p_index_metric
    union
    /* for cost center granularity */
    select
    'cc',
    'FlexFTEcc',
    'FlexTrgtCc',
    'PrdctvtyIndexCc'
),

fte_denominator as ( /* capture the FTEs for index */
    select
        metric_mapping.p_index_metric,
        metric_mapping.target_metric,
        metric_mapping.fte_metric,
        fte_row.metric_dt_key,
        cost_center_id,
        job_group_id,
        numerator as index_denominator,
        metric_mapping.granularity
    from
        {{ ref('stg_nursing_time_w6_flex_gap') }} as fte_row
        inner join metric_mapping
            on fte_row.metric_abbreviation = metric_mapping.fte_metric
),

add_target_calc_index as (
     /* match to the target at the granualrity, productivity index is target/actual */
    select
        fte_denominator.p_index_metric as metric_abbreviation,
        fte_denominator.target_metric,
        fte_denominator.fte_metric,
        fte_denominator.metric_dt_key,
        fte_denominator.cost_center_id,
        fte_denominator.job_group_id,
        trget_row.numerator,
        fte_denominator.index_denominator as denominator,
        round(trget_row.numerator
            / fte_denominator.index_denominator, 3) as row_metric_calculation
    from
        fte_denominator
        inner join {{ ref('stg_nursing_time_w6_flex_gap') }} as trget_row
            on fte_denominator.target_metric = trget_row.metric_abbreviation
            and fte_denominator.metric_dt_key = trget_row.metric_dt_key
            and fte_denominator.cost_center_id = trget_row.cost_center_id
            and coalesce(fte_denominator.job_group_id, 'NULL') = coalesce(trget_row.job_group_id, 'NULL')
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    job_group_id as metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
    add_target_calc_index
