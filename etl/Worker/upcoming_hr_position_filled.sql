{{
  config(
    meta = {
      'critical': false
    }
  )
}}

/* hr_position_filled
enterprise table for people coming into CHOP or transferring positions and
showing the prior job//cost center/fte when applicable
Note: rows roll off when the start date arrives
*/
select
    hr_position_filled.current_chop_job_code,
    hr_position_filled.current_cost_center_id,
    hr_position_filled.current_fte_percentage,
    hr_position_filled.worker_id,
    case
        when internal_ind = 1
        then worker.display_name
    end as internal_transfer_display_name,
    hr_position_filled.filled_job_code,
    hr_position_filled.filled_cost_center_id,
    hr_position_filled.filled_fte_percentage,
    hr_position_filled.same_cost_center_ind,
    hr_position_filled.job_start_date,
    hr_position_filled.location,
    hr_position_filled.hiring_manager_full_name,
    curr_cc.cost_center_display as current_cost_center,
    position_cc.cost_center_display as new_position_cost_center,
    case
        when coalesce(curr_cc.has_nursing_current_year_budget_ind, 0) = 1
            or position_cc.has_nursing_current_year_budget_ind = 1
        then 1
        else 0
    end as nursing_impact_ind,
    hr_position_filled.scheduled_weekly_hours,
    hr_position_filled.position_filled_ind,
    hr_position_filled.application_id,
    hr_position_filled.internal_ind,
    hr_position_filled.job_req_id,
    hr_position_filled.position_code,
    hr_position_filled.position_filled_date,
    hr_position_filled.recruiter_full_name,
    hr_position_filled.chop_vice_president,
    hr_position_filled.current_application_status,
    hr_position_filled.days_to_fill,
    hr_position_filled.days_to_accept,
    hr_position_filled.days_in_preboarding,
    hr_position_filled.days_to_start,
    hr_position_filled.current_fte_percentage / 100 as current_fte,
    hr_position_filled.filled_fte_percentage / 100 as filled_fte
from
    {{ ref('stg_hr_recruiting_position') }} as hr_position_filled
    left join {{ ref('worker') }} as worker
        on hr_position_filled.worker_id = worker.worker_id
    left join {{ ref('nursing_cost_center_attributes') }} as curr_cc
        on hr_position_filled.current_cost_center_id = curr_cc.cost_center_id
    left join {{ ref('nursing_cost_center_attributes') }} as position_cc
        on hr_position_filled.filled_cost_center_id = position_cc.cost_center_id
where
    position_filled_ind = 1
