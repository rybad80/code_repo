/* stg_nursing_w_k2_replacement_factor
creates the replacement factor fiscal year target and replacement factor percent
for the staff nurse rollup
the target can only be applied to staff nurses
*/

with
get_target as (
    select
        trgt.fiscal_year,
        trgt.staff_nurse_replacement_factor_target,
        nursing_pay_period.pp_end_dt_key
    from {{ ref('lookup_nursing_target_replacement_factor') }} as trgt
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on trgt.fiscal_year = nursing_pay_period.fiscal_year
),

replacement_factor_target as (
    select
        'NonDirectTrgtPctsNurse' as metric_abbreviation,
        stg_nursing_time_w4_percent.metric_dt_key,
        stg_nursing_time_w4_percent.worker_id,
        stg_nursing_time_w4_percent.cost_center_id,
        stg_nursing_time_w4_percent.cost_center_site_id,
        stg_nursing_time_w4_percent.job_code,
        stg_nursing_job_group_levels.job_group_level_4_id as job_group_id,
        stg_nursing_job_group_levels.job_group_level_4_id as metric_grouper,
        get_target.staff_nurse_replacement_factor_target as numerator,
        null::numeric as denominator,
        get_target.staff_nurse_replacement_factor_target as row_metric_calculation
    from {{ ref('stg_nursing_time_w4_percent') }} as stg_nursing_time_w4_percent
    inner join get_target
        on stg_nursing_time_w4_percent.metric_dt_key = get_target.pp_end_dt_key
    inner join {{ ref('stg_nursing_job_group_levels') }} as stg_nursing_job_group_levels
        on stg_nursing_time_w4_percent.job_group_id = stg_nursing_job_group_levels.job_group_id
    where stg_nursing_time_w4_percent.metric_abbreviation = 'NonDirectPctofsNurse'
    group by
        stg_nursing_time_w4_percent.metric_dt_key,
        stg_nursing_time_w4_percent.worker_id,
        stg_nursing_time_w4_percent.cost_center_id,
        stg_nursing_time_w4_percent.cost_center_site_id,
        stg_nursing_time_w4_percent.job_code,
        stg_nursing_job_group_levels.job_group_level_4_id,
        stg_nursing_job_group_levels.job_group_level_4_id,
        get_target.staff_nurse_replacement_factor_target
),

replacement_factor_staff_nurse_pct as (
    select
        'NonDirectKPIpctsNurse' as metric_abbreviation,
        stg_nursing_time_w4_percent.metric_dt_key,
        stg_nursing_time_w4_percent.worker_id,
        stg_nursing_time_w4_percent.cost_center_id,
        stg_nursing_time_w4_percent.cost_center_site_id,
        stg_nursing_time_w4_percent.job_code,
        stg_nursing_job_group_levels.job_group_level_4_id as job_group_id,
        stg_nursing_job_group_levels.job_group_level_4_id as metric_grouper,
        sum(stg_nursing_time_w4_percent.numerator) as numerator,
        sum(stg_nursing_time_w4_percent.denominator) as denominator,
        sum(stg_nursing_time_w4_percent.numerator)
            / sum(stg_nursing_time_w4_percent.denominator) as row_metric_calculation
    from {{ ref('stg_nursing_time_w4_percent') }} as stg_nursing_time_w4_percent
    inner join {{ ref('stg_nursing_job_group_levels') }} as stg_nursing_job_group_levels
        on stg_nursing_time_w4_percent.job_group_id = stg_nursing_job_group_levels.job_group_id
    where metric_abbreviation = 'NonDirectPctofsNurse'
    group by
        stg_nursing_time_w4_percent.metric_dt_key,
        stg_nursing_time_w4_percent.worker_id,
        stg_nursing_time_w4_percent.cost_center_id,
        stg_nursing_time_w4_percent.cost_center_site_id,
        stg_nursing_time_w4_percent.job_code,
        stg_nursing_job_group_levels.job_group_level_4_id
)

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
from replacement_factor_target

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
from replacement_factor_staff_nurse_pct
