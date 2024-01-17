{{ config(meta = {
    'critical': false
}) }}

/* position_control_job_filled
describes nursing impacted scenarios due to a person transferring
to a new job code or new cost center or a new person taking a
job in a position control cost center
*/

select
    hr_position_filled.current_chop_job_code,
    hr_position_filled.current_cost_center_id,
    hr_position_filled.current_fte,
    coalesce(curr_cc.has_nursing_current_year_budget_ind, 0) = 1 as position_control_transfer_out_ind,
    coalesce(position_cc.has_nursing_current_year_budget_ind, 0) = 1 as position_control_filled_job_ind,
    trim(leading 'JOB_APPLICATION-' from application_id) as application_id_parsed,
    hr_position_filled.worker_id,
    hr_position_filled.filled_job_code,
    hr_position_filled.filled_cost_center_id,
    hr_position_filled.filled_fte,
    hr_position_filled.same_cost_center_ind,
    hr_position_filled.job_start_date,
    hr_position_filled.location,
    hr_position_filled.hiring_manager_full_name,
    hr_position_filled.current_cost_center,
    hr_position_filled.new_position_cost_center,
    hr_position_filled.nursing_impact_ind,
    hr_position_filled.job_req_id,
    hr_position_filled.position_code,
    hr_position_filled.application_id,
    hr_position_filled.internal_ind,
    hr_position_filled.chop_vice_president,
    hr_position_filled.current_application_status,
    curr_job.job_title_display as current_job_title_display,
    curr_job.nursing_job_grouper as current_job_group,
    filled_job.job_title_display as filled_job_title_display,
    filled_job.nursing_job_grouper as filled_job_group
from
   {{ ref('upcoming_hr_position_filled') }} as hr_position_filled
left join {{ ref('nursing_cost_center_attributes') }} as curr_cc
    on hr_position_filled.current_cost_center_id = curr_cc.cost_center_id
left join {{ ref('nursing_cost_center_attributes') }} as position_cc
    on hr_position_filled.filled_cost_center_id = position_cc.cost_center_id
left join {{ ref('stg_nursing_job_code_group') }} as curr_job
    on hr_position_filled.current_chop_job_code = curr_job.job_code
left join {{ ref('stg_nursing_job_code_group') }} as filled_job
    on hr_position_filled.filled_job_code = filled_job.job_code
where hr_position_filled.nursing_impact_ind = 1
    and (coalesce(curr_cc.has_nursing_current_year_budget_ind, 0) = 1
        or position_cc.has_nursing_current_year_budget_ind = 1)
    and (curr_cc.cost_center_id is not null or position_cc.cost_center_id  is not null)
