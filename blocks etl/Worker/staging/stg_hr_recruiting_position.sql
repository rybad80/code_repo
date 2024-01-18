{{
  config(
    meta = {
      'critical': false
    }
  )
}}

/* stg_hr_recruiting_position
using the Workday New Employee Orientation report capture the position
filled data, for various stages of the recruitment process
and if the person is already at CHOP and moving to a new
job or cost center, capture what that current information is from worker
*/
select
    case coalesce(worker.active_ind, 0)
        when 1
        then worker.job_code
    end as current_chop_job_code,
    case coalesce(worker.active_ind, 0)
        when 1
        then worker.cost_center_id
    end as current_cost_center_id,
    case coalesce(worker.active_ind, 0)
        when 1
        then worker.fte_percentage
    end as current_fte_percentage,
    neo.job_code as filled_job_code,
    neo.employee_id as worker_id,
    substr(neo.cost_center, 1, 5) as filled_cost_center_id,
    case
        when current_cost_center_id = filled_cost_center_id
        then 1
        else 0
    end as same_cost_center_ind,
    date(neo.job_start_date) as job_start_date,
    neo.bi_weekly_hours / 2 as scheduled_weekly_hours,
    round(neo.bi_weekly_hours::numeric / 80 * 100, 3) as filled_fte_percentage,
    case neo.current_application_status
        when 'Preboarding' then 1
        when 'Ready for Hire' then 1
        else 0
    end as position_filled_ind,
    neo.application_id,
    neo.is_internal as internal_ind,
    neo.job_req_id,
    neo.position_code,
    neo.hired_on as position_filled_date,
    neo.location,
    neo.recruiter_full_name,
    neo.hiring_manager_full_name,
    neo.vp_name as chop_vice_president,
    neo.current_application_status,
    neo.time_to_fill as days_to_fill,
    neo.time_to_accept as days_to_accept,
    neo.time_in_preboarding as days_in_preboarding,
    neo.time_to_start as days_to_start
from
    {{ source('workday_ods', 'cr_workday_neo') }}  as neo /* new_employee_orientation */
    left join {{ ref('worker') }} as worker
        on neo.employee_id = worker.worker_id
