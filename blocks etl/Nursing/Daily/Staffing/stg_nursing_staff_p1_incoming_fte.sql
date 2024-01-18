/*
stg_nursing_staff_p1_incoming_fte
worker level granularity for the  upcoming adds of a person for a job for a cost center
at a certain FTE (full time equivalency) for an effective date (& get job group)
*/

select
    'StaffWrkrAdd' as metric_abbreviation,
    to_char(unit_add.job_start_date, 'yyyymmdd') as metric_dt_key,
    case /* only keep worker ID if the perosn is already with CHOP */
        when unit_add.internal_ind = 1
        then unit_add.worker_id
    end as worker_id,
    unit_add.internal_ind,
    unit_add.filled_cost_center_id as cost_center_id,
    unit_add.filled_job_code as job_code,
    stg_nursing_job_code_group.provider_job_group_id as job_group_id,
    stg_nursing_job_code_group.additional_job_group_info as metric_grouper,
    sum(unit_add.filled_fte) as numerator
from
    {{ ref('upcoming_hr_position_filled') }} as unit_add
        left join {{ ref('stg_nursing_job_code_group') }} as stg_nursing_job_code_group
        on unit_add.filled_job_code = stg_nursing_job_code_group.job_code
where
     nursing_impact_ind =  1
group by
    metric_dt_key,
    unit_add.worker_id,
    unit_add.internal_ind,
    unit_add.filled_cost_center_id,
    unit_add.filled_job_code,
    stg_nursing_job_code_group.provider_job_group_id,
    stg_nursing_job_code_group.additional_job_group_info,
    current_chop_job_code
