{{ config(meta = {
    'critical': true
}) }}

/* stg_worker_callback_pp_hours
total hours of callback Kronos time
for a pay period for each worker with their nursing job rollup,
job_group and future pay period indicator
*/
 select
        nursing_pay_period.pp_end_dt_key,
        callback_hrs.cost_center_id,
        callback_hrs.job_group_id,
        callback_hrs.worker_id,
        nursing_pay_period.future_pay_period_ind,
        coalesce(get_job_rollup.staff_nurse_ind, 0) as staff_nurse_ind,
        get_job_rollup.nursing_job_rollup,
        sum(callback_hrs.productive_direct_daily_hours) as productive_direct_pp_hours
    from
        {{ ref('stg_timereport_daily_callback') }} as callback_hrs
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on callback_hrs.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
        left join {{ ref('job_group_levels_nursing') }} as get_job_rollup
            on callback_hrs.job_group_id = get_job_rollup.job_group_id

    group by
        callback_hrs.pp_end_dt_key,
        nursing_pay_period.pp_end_dt_key,
        callback_hrs.cost_center_id,
        callback_hrs.job_group_id,
        callback_hrs.worker_id,
        nursing_pay_period.future_pay_period_ind,
        coalesce(get_job_rollup.staff_nurse_ind, 0),
        get_job_rollup.nursing_job_rollup
