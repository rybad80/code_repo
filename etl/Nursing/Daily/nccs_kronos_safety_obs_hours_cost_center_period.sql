{{ config(meta = {
    'critical': true
}) }}

/* nccs_kronos_safety_obs_hours_cost_center_period
aggregates by subtrahend_for_hppd_numerator_ind, pay
period, role/job, and cost center
*/
select
    cost_center_id,
    job_group_id as job_role_rollup,
    pp_end_dt_key as pay_period_end_dt_key,
    round(sum(safety_obs_hours), 4) as safety_obs_hours,
    sum(safety_obs_fte) as safety_obs_fte,
    case
        when hppd_job_group_id = 'UAP' then 1
        else 0
    end as subtrahend_for_hppd_numerator_ind,
    rn_job_ind
from
    {{ ref('stg_nccs_safety_obs_role_indicators') }}
group by
    cost_center_id,
    job_group_id,
    hppd_job_group_id,
    pp_end_dt_key,
    subtrahend_for_hppd_numerator_ind,
    rn_job_ind
