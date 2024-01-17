/* stg_nursing_staff_worker_selection
gather all the active CHOP people with most applicable job_group
and include job group from an alternate lens when applicable
*/
with
curr_pp as (
    select
        pp_end_dt_key as metric_dt_key,
        pp_end_dt
    from
        {{ ref('nursing_pay_period') }}
    where
        current_working_pay_period_ind = 1
)

select
    curr_pp.metric_dt_key,
    worker.worker_id,
    worker.cost_center_id,
    worker.cost_center_site_id,
    worker.job_code,
    stg_nursing_job_code_group_statistic.use_job_group_id as job_group_id,
    case
        when stg_nursing_job_code_group_statistic.fixed_rn_override_ind = 1
        then stg_nursing_job_code_group_statistic.rn_alt_job_group_id
        else stg_nursing_job_code_group_statistic.use_job_group_id
    end as staffing_use_job_group_id,
    stg_nursing_job_code_group_statistic.additional_job_group_info,
    worker.active_ind,
    worker.fte_percentage
from
    {{ ref('worker') }} as worker
    /*  inner join chop_analytics..nursing_cost_center_attributes as cc
        on worker.cost_center_id = cc.cost_center_id  */
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as stg_nursing_job_code_group_statistic
        on worker.job_code = stg_nursing_job_code_group_statistic.job_code
    left join {{ ref('job_group_levels_nursing') }} as job_group_levels_nursing
        on case
            when stg_nursing_job_code_group_statistic.fixed_rn_override_ind = 1
            then stg_nursing_job_code_group_statistic.rn_alt_job_group_id
            else stg_nursing_job_code_group_statistic.use_job_group_id
        end = job_group_levels_nursing.job_group_id
    inner join curr_pp
        on worker.hire_date <= curr_pp.pp_end_dt
where
    worker.active_ind = 1
    and worker.employee_ind = 1
