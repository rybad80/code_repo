{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_w1b_extrapay
set the metric abbreviation and aggregate the incentive pay
"hours" total for the job group/pay period for each cost center
*/
with extra_pay_src as (
    select
        case future_pay_period_ind when 1 then 'Upcoming' else '' end
            || 'ExtraPayHrs' as metric_abbreviation,
        stg_nursing_extra_pay_p1_daily_worker.metric_dt_key,
        stg_nursing_extra_pay_p1_daily_worker.cost_center_id,
        stg_nursing_extra_pay_p1_daily_worker.job_group_id,
        stg_nursing_extra_pay_p1_daily_worker.sum_period_hrs as extra_pay_hour_plus
    from
        {{ ref('stg_nursing_extrapay_p2_pp_job_group') }} as stg_nursing_extra_pay_p1_daily_worker
                inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on stg_nursing_extra_pay_p1_daily_worker.metric_dt_key = nursing_pay_period.pp_end_dt_key
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
    extra_pay_hour_plus as numerator,
    null::numeric as denominator,
    extra_pay_hour_plus as row_metric_calculation
from
    extra_pay_src
