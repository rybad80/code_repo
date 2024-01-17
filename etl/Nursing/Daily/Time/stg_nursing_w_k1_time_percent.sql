/* stg_nursing_w_k1_time_percent
calculates the percent of total time for productive direct, productive indirect,
and non-productive time off hours at the job group leaf granularity for all job groups
*/

with
time_category_hrs as (
    select
        metric_abbreviation,
        metric_dt_key,
        worker_id,
        cost_center_id,
        cost_center_site_id,
        job_code,
        job_group_id,
        metric_grouper,
        row_metric_calculation as time_by_category
    from {{ ref('stg_nursing_time_w1a_hrs_direct') }}
    where metric_abbreviation = 'DirectHrs'

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
        row_metric_calculation as time_by_category
    from {{ ref('stg_nursing_time_sum_final') }}
    where metric_abbreviation in ('IndirectProductiveHrs', 'TimeNonProdHrs')
),

total_time_hrs as (
    select
        metric_abbreviation,
        metric_dt_key,
        worker_id,
        cost_center_id,
        cost_center_site_id,
        job_code,
        job_group_id,
        metric_grouper,
        row_metric_calculation as total_time
    from {{ ref('stg_nursing_time_sum_final') }}
    where metric_abbreviation = 'CountedTotalHrs'
)

select
    case time_category_hrs.metric_abbreviation
        when 'DirectHrs'
            then 'DirectProdPct'
        when 'IndirectProductiveHrs'
            then 'IndirectProdPct'
        when 'TimeNonProdHrs'
            then 'NonProdPct'
    end as metric_abbreviation,
    time_category_hrs.metric_dt_key as metric_dt_key,
    time_category_hrs.worker_id as worker_id,
    time_category_hrs.cost_center_id as cost_center_id,
    time_category_hrs.cost_center_site_id as cost_center_site_id,
    time_category_hrs.job_code as job_code,
    time_category_hrs.job_group_id as job_group_id,
    time_category_hrs.metric_grouper as metric_grouper,
    time_category_hrs.time_by_category as numerator,
    total_time_hrs.total_time as denominator,
    case when (total_time_hrs.total_time = 0
        or total_time_hrs.total_time is null)
        then null
        else time_category_hrs.time_by_category / total_time_hrs.total_time
    end as row_metric_calculation
from time_category_hrs
left join total_time_hrs
    on time_category_hrs.metric_dt_key = total_time_hrs.metric_dt_key
    and time_category_hrs.cost_center_id = total_time_hrs.cost_center_id
    and time_category_hrs.job_group_id = total_time_hrs.job_group_id
    and time_category_hrs.metric_grouper = total_time_hrs.metric_grouper
