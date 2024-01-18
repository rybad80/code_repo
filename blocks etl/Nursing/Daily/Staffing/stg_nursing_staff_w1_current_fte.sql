/* stg_nursing_staff_w1_current_fte
capture the worker job code counts (currWrkr)
and the FTE totals by job_gorup and cost_center (currFTE)
and for (currFTElvl4)
roll those up as needed to Lvl 4 job_group granularity
in order to prepare for vacancy FTE & rate calculations
See the stg_nursing_staff_w2_budget for more comments on this
*/
with
curr_wrkr as (
select
    'currWrkr' as metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    cost_center_site_id,
    job_code,
    staffing_use_job_group_id as job_group_id,
    additional_job_group_info as metric_grouper,
    sum(active_ind) as numerator,
    null::numeric as denominator,
    sum(active_ind) as row_metric_calculation
from
    {{ ref('stg_nursing_staff_worker_selection') }}
group by
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    cost_center_site_id,
    job_code,
    staffing_use_job_group_id,
    additional_job_group_info
),

curr_fte as (
select
    'currFTE' as metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    staffing_use_job_group_id as job_group_id,
    additional_job_group_info as metric_grouper,
    fte_percentage / 100 as numerator,
    null::numeric as denominator,
    fte_percentage / 100 as row_metric_calculation
from
    {{ ref('stg_nursing_staff_worker_selection') }}
),

curr_fte_lvl4 as (
    select
        'currFTElvl4' as metric_abbreviation,
        curr_fte.metric_dt_key,
        curr_fte.cost_center_id,
        coalesce(
            --lvls.level_4_id,
            --lvls.job_group_id,
            lvls.nursing_job_rollup,
            curr_fte.job_group_id,
            'unk Job Grp ID') as job_group_id,
        sum(numerator) as numerator,
        null::numeric as denominator,
        sum(numerator) as row_metric_calculation
    from curr_fte
    left join {{ ref('job_group_levels_nursing') }} as lvls
        on curr_fte.job_group_id = lvls.job_group_id
    where curr_fte.metric_abbreviation = 'currFTE'
    group by
        curr_fte.metric_dt_key,
        curr_fte.cost_center_id,
        coalesce(--lvls.level_4_id, lvls.job_group_id, 
        lvls.nursing_job_rollup, curr_fte.job_group_id, 'unk Job Grp ID')
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from curr_wrkr

union all

select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from curr_fte

union all

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from curr_fte_lvl4
