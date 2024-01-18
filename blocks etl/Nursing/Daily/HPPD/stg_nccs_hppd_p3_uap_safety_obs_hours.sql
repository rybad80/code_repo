{{ config(meta = {
    'critical': true
}) }}

/* stg_nccs_hppd_p3_uap_safety_obs_hours
capture the safety obs hours by UAPs
because Unlicensed Assistive Personnel doing safety obs cannot be doing other patient
care assistance on the unit because it is 1 on 1 time with a single patient
--> collected here so can be removed from HPPD numerator
*/
with reduce_total_hours_by_uap_saftey_obs_hours as (
    select
        pay_period_end_dt_key,
        cost_center_id,
        sum(safety_obs_hours) as uap_safety_obs_hours
    from
        {{ ref('nccs_kronos_safety_obs_hours_cost_center_period') }}
    where
        subtrahend_for_hppd_numerator_ind = 1
    group by
        pay_period_end_dt_key,
        cost_center_id
),

hppd_cc_pp as (
    select
        pp_end_dt_key,
        cost_center_id,
        sum(hppd_hours) as total_direct_hours
    from
        {{ ref('stg_nccs_hppd_p1_direct_productive') }}
    group by
        pp_end_dt_key,
        cost_center_id
)

select
    hppd_cc_pp.pp_end_dt_key,
    hppd_cc_pp.cost_center_id,
    'UAP' as metric_grouper,
    coalesce(safety_obs_hours.uap_safety_obs_hours, 0) as uap_safety_obs_hours
from
    hppd_cc_pp
    left join reduce_total_hours_by_uap_saftey_obs_hours as safety_obs_hours
        on hppd_cc_pp.cost_center_id = safety_obs_hours.cost_center_id
        and hppd_cc_pp.pp_end_dt_key = safety_obs_hours.pay_period_end_dt_key
