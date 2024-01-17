/* nccs_kronos_extra_pay_hours
gather worker record for extra pay "hours"/FTE to work a shift per the
inducement paycode from the Kronos record
*/
with date_max_selector as (
    select
        max(pp_end_dt_key) as to_get_thru_next_schedule_period_end_dt
    from
        {{ ref('nursing_pay_period') }}
    where
        next_schedule_period_ind = 1
),

extra_pay_hours_sum as (
    select
        worker_extrapay.cost_center_id,
        worker_extrapay.pp_end_dt_key,
        worker_extrapay.timereport_paycode,
        worker_extrapay.worker_id,
        worker_extrapay.job_code,
        worker_extrapay.job_organization_id,
        worker_extrapay.orgpath_safety_obs_ind,
        worker_extrapay.orgpath_meal_out_of_room_ind,
        coalesce(job_rollup.nursing_job_rollup, 'unknown') as job_role_rollup,
        sum(worker_extrapay.extra_pay_hour_plus) as extra_pay_hours,
        sum(worker_extrapay.extra_pay_hour_plus) / 80 as extra_pay_fte
    from
        {{ ref('stg_nursing_extrapay_p1_daily_worker') }} as worker_extrapay
        inner join date_max_selector on worker_extrapay.pp_end_dt_key
            <= date_max_selector.to_get_thru_next_schedule_period_end_dt
        left join {{ ref('job_group_levels_nursing') }} as job_rollup
            on worker_extrapay.job_group_id = job_rollup.job_group_id
    group by
        worker_extrapay.cost_center_id,
        worker_extrapay.pp_end_dt_key,
        worker_extrapay.worker_id,
        worker_extrapay.job_code,
        worker_extrapay.job_organization_id,
        worker_extrapay.orgpath_safety_obs_ind,
        worker_extrapay.orgpath_meal_out_of_room_ind,
        worker_extrapay.timereport_paycode,
        coalesce(job_rollup.nursing_job_rollup, 'unknown')
)

select
    worker.display_name as worker_nm,
    extra_pay_hours_sum.worker_id,
    extra_pay_hours_sum.cost_center_id,
    extra_pay_hours_sum.job_role_rollup,
    extra_pay_hours_sum.job_code,
    extra_pay_hours_sum.pp_end_dt_key as pay_period_end_dt_key,
    extra_pay_hours_sum.timereport_paycode,
    extra_pay_hours_sum.extra_pay_hours,
    extra_pay_hours_sum.extra_pay_fte,
    extra_pay_hours_sum.job_organization_id,
    extra_pay_hours_sum.orgpath_safety_obs_ind,
    extra_pay_hours_sum.orgpath_meal_out_of_room_ind,
    nursing_pay_period.future_pay_period_ind as future_ind
from
    extra_pay_hours_sum
    inner join {{ ref('worker') }} as worker
        on extra_pay_hours_sum.worker_id = worker.worker_id
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on extra_pay_hours_sum.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
