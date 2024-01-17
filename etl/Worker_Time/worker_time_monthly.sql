{{ config(meta = {
    'critical': false
}) }}
/* worker_time_monthly
Enterprise time worked and time-off (non-productive) aggregates monthly
for cost center & site and per worker with job_group rollup
for the subsets under Productive Direct, Productive Indirect, and Time off
*/
select
    time_data.cost_center_id,
    time_data.cost_center_site_id,
    time_data.time_subset,
    date_trunc('month', time_data.metric_date) as time_month,
    time_data.date_fiscal_year,
    time_data.worker_id,
    job_rollup.use_job_group_id as job_group_id,

    case
        when productive_direct_ind = 1 then 'Direct'
        when productive_indirect_ind = 1 then 'Indirect'
        when non_productive_ind = 1 then 'Time-Off'
        else 'n/a'
    end as productive_time_category,
    sum(time_data.worker_daily_total) as month_hours,
    time_data.productive_direct_ind,
    time_data.productive_indirect_ind,
    time_data.non_productive_ind,
    time_data.dart_time_worked_ind,
    time_data.callback_ind,
    time_data.productivity_type_id
from
    {{ ref('timereport_daily_all') }} as time_data
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as job_rollup
        on time_data.job_code = job_rollup.job_code
where
    /* ensure it is counted time */
    time_data.productive_direct_ind
    + time_data.productive_indirect_ind
    + time_data.non_productive_ind
    + time_data.callback_ind > 0
group by
    time_data.cost_center_id,
    time_data.cost_center_site_id,
    time_data.time_subset,
    date_trunc('month', time_data.metric_date),
    time_data.date_fiscal_year,
    time_data.worker_id,
    job_rollup.use_job_group_id,
    time_data.productive_direct_ind,
    time_data.productive_indirect_ind,
    time_data.non_productive_ind,
    time_data.dart_time_worked_ind,
    time_data.callback_ind,
    time_data.productivity_type_id
