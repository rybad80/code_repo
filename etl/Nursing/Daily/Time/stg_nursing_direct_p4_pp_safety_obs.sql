{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_direct_p4_pp_safety_obs
compile the safety obs metric rows at the job group id level
as well as rolled up to the safety observer level 4
so that it can be compared to the budget that is at that level 4
Note safety obs excludes the RNs since RN instances mean they are 1 on 1 with the
patient but can also do other RN patient care while with that patient.
*/

with safetyobshours_detail as (
    select
        cost_center_id,
        pay_period_end_dt_key,
        job_role_rollup as job_group_id,
        sum(safety_obs_hours) as job_group_safety_obs_hours
    from
        {{ ref('nccs_kronos_safety_obs_hours_cost_center_period') }}
    where
        rn_job_ind = 0
    group by
        cost_center_id,
        pay_period_end_dt_key,
        job_role_rollup
),

safetyobshours_total as (
    select
        safetyobshours_detail.cost_center_id,
        safetyobshours_detail.pay_period_end_dt_key,
		jg_lvls.nursing_job_rollup as job_group_id,
        sum(job_group_safety_obs_hours) as total_safety_obs_hours
    from
        safetyobshours_detail
		inner join {{ ref('job_group_levels_nursing') }} as jg_lvls
		on jg_lvls.job_group_id = 'psychTech'
    group by
        safetyobshours_detail.cost_center_id,
        safetyobshours_detail.pay_period_end_dt_key,
		jg_lvls.nursing_job_rollup
)

select
    case nursing_pay_period.future_pay_period_ind
        when 1
        then 'Upcoming' else ''
        end
    || 'SafetyObsJgHrs' as metric_abbreviation,
    pay_period_end_dt_key as metric_dt_key,
    cost_center_id,
    job_group_id,
    job_group_safety_obs_hours as numerator
from
    safetyobshours_detail
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
     on safetyobshours_detail.pay_period_end_dt_key = nursing_pay_period.pp_end_dt_key

union all

select
    case nursing_pay_period.future_pay_period_ind
        when 1
        then 'Upcoming' else ''
        end
    || 'SafetyObsHrs' as metric_abbreviation,
    pay_period_end_dt_key as metric_dt_key,
    cost_center_id,
    job_group_id,
    total_safety_obs_hours as numerator
from
    safetyobshours_total
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
     on safetyobshours_total.pay_period_end_dt_key = nursing_pay_period.pp_end_dt_key
