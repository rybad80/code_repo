/*
stg_nursing_staff_p2_outgoing_fte
worker level granularity for the upcoming loss of a person for a job for a cost center
at a certain FTE (full time equivalency) for an effective date (& get job group)
because the person is transferring to a new role at CHOP (and later also to include
future terminations)
*/

select
    'StaffWrkrMinus' as metric_abbreviation,
    to_char(unit_subtract.job_start_date, 'yyyymmdd') as metric_dt_key,
    unit_subtract.worker_id,
    unit_subtract.internal_ind,
    unit_subtract.current_cost_center_id as cost_center_id,
    unit_subtract.current_chop_job_code as job_code,
    stg_nursing_job_code_group.provider_job_group_id as job_group_id,
    stg_nursing_job_code_group.additional_job_group_info as metric_grouper,
    sum(-unit_subtract.current_fte) as numerator
from
    {{ ref('upcoming_hr_position_filled') }} as unit_subtract
        left join {{ ref('stg_nursing_job_code_group') }} as stg_nursing_job_code_group
        on unit_subtract.current_chop_job_code = stg_nursing_job_code_group.job_code
where
    unit_subtract.current_chop_job_code is not null
    and nursing_impact_ind =  1
group by
    metric_dt_key,
    unit_subtract.worker_id,
    unit_subtract.internal_ind,
    unit_subtract.current_cost_center_id,
    unit_subtract.current_chop_job_code,
    stg_nursing_job_code_group.provider_job_group_id,
    stg_nursing_job_code_group.additional_job_group_info
