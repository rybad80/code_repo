{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_extrapay_p2_pp_job_group
set the metric abbreviation and aggregate the incentive pay
"hours" total for the job group/pay period for each cost center
*/
with extra_pay_src as (
    select
        case future_pay_period_ind when 1 then 'Upcoming' else '' end
            || 'ExtraPayHrs' as metric_abbreviation,
        stg_nursing_extra_pay_p1_daily_worker.pp_end_dt_key as metric_dt_key,
        stg_nursing_extra_pay_p1_daily_worker.cost_center_id,
        stg_nursing_extra_pay_p1_daily_worker.job_group_id,
        stg_nursing_extra_pay_p1_daily_worker.extra_pay_hour_plus,
        stg_nursing_extra_pay_p1_daily_worker.extra_pay_hour_plus_fte
    from
        {{ ref('stg_nursing_extrapay_p1_daily_worker') }} as stg_nursing_extra_pay_p1_daily_worker
                inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on stg_nursing_extra_pay_p1_daily_worker.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
)

    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        sum(extra_pay_hour_plus) as sum_period_hrs
    from
        extra_pay_src
    group by
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id
