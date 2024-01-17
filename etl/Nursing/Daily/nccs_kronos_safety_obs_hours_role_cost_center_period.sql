{{ config(meta = {
    'critical': true
}) }}

/* nccs_kronos_safety_obs_hours_role_cost_center_period
capture pay period worker aggreagates for safety observation time recorded in Kronos
with various indicators & groupers to utilize for in app selections
*/
select
    case future_pay_period_ind when 1 then 'Upcoming'
        else '' end || 'SafetyObsPP' as metric_abbreviation,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    nursing_job_grouper,
    job_role_grouper,
    pp_end_dt_key,
    sum(safety_obs_hours) as safety_obs_hours,
    sum(safety_obs_fte) as safety_obs_fte,
    orgpath_meal_out_of_room_ind,
    orgpath_safety_obs_ind,
    orgpath_bhc_charge_ind,
    one_on_one_safety_obs_job_ind,
    overtime_ind,
    timejob_abbreviation
from
    {{ ref('stg_nccs_safety_obs_role_indicators') }}
group by
    future_pay_period_ind,
    worker_id,
    cost_center_id,
    cost_center_site_id,
    job_code,
    job_group_id,
    nursing_job_grouper,
    job_role_grouper,
    pp_end_dt_key,
    orgpath_meal_out_of_room_ind,
    orgpath_safety_obs_ind,
    orgpath_bhc_charge_ind,
    one_on_one_safety_obs_job_ind,
    overtime_ind,
    timejob_abbreviation
