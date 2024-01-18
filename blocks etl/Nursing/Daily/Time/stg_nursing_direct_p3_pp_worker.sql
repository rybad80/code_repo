{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_direct_p3_pp_worker
aggregate pay period rollup to the hours by pay code by worker
for the productive direct (REGULAR and OVERTIME) time
*/

select
        nursing_pay_period.prior_pay_period_ind,
        nursing_pay_period.future_pay_period_ind,
        productive_hours.pp_end_dt_key as metric_dt_key,
        productive_hours.worker_id,
        productive_hours.cost_center_id,
        productive_hours.job_code,
        productive_hours.job_group_id,
        sum(productive_hours.productive_direct_daily_hours) as pp_sum_hours,
        productive_hours.timereport_paycode as pay_code
    from
        {{ ref('timereport_daily_productive_direct') }} as productive_hours
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on productive_hours.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
    group by
        nursing_pay_period.prior_pay_period_ind,
        nursing_pay_period.future_pay_period_ind,
        productive_hours.pp_end_dt_key,
        productive_hours.worker_id,
        productive_hours.cost_center_id,
        productive_hours.job_code,
        productive_hours.job_group_id,
        productive_hours.timereport_paycode
