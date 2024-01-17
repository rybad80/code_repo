{{ config(meta = {
    'critical': true
}) }}

/* stg_nccs_hppd_p4_use_direct_hours
add the Acute RN & the UAP adjusted hours
together to use in HPPD_w1 & w2 (HPPDhours)
and the RN and UAP and subrtrahend called out also as individual metrics
*/
with direct_hours_by_group_abbr as (
    select
        direct_hrs.pp_end_dt_key as metric_dt_key,
        direct_hrs.cost_center_id,
        direct_hrs.grp_for_hppd_abbr as metric_grouper,
        sum(direct_hrs.hppd_hours) as direct_hours
    from
        {{ ref('stg_nccs_hppd_p1_direct_productive') }} as direct_hrs
        inner join {{ ref('nursing_cost_center_attributes') }} as hppd_cc
            on direct_hrs.cost_center_id = hppd_cc.cost_center_id
            and hppd_cc.hppd_ind = 1
    group by
        direct_hrs.pp_end_dt_key,
        direct_hrs.cost_center_id,
        direct_hrs.grp_for_hppd_abbr
),

cc_pp_rn_plus_uap_hours as (
    select
        metric_dt_key,
        cost_center_id,
        sum(direct_hours) as rn_plus_uap_hours
    from
        direct_hours_by_group_abbr
    group by
        metric_dt_key,
        cost_center_id
),

metric_row as (
    select
        'HPPDhours' as metric_abbreviation,
        cc_pp_rn_plus_uap_hours.metric_dt_key,
        cc_pp_rn_plus_uap_hours.cost_center_id,
        null as metric_grouper,
        cc_pp_rn_plus_uap_hours.rn_plus_uap_hours
        - coalesce(safety_obs_hours.uap_safety_obs_hours, 0) as numerator
    from
        cc_pp_rn_plus_uap_hours
        left join {{ ref('stg_nccs_hppd_p3_uap_safety_obs_hours') }} as safety_obs_hours
            on cc_pp_rn_plus_uap_hours.cost_center_id = safety_obs_hours.cost_center_id
            and cc_pp_rn_plus_uap_hours.metric_dt_key = safety_obs_hours.pp_end_dt_key

    union all

    select
        'RNhours' as metric_abbreviation,
         metric_dt_key,
        cost_center_id,
        metric_grouper,
        direct_hours as numerator
    from
        direct_hours_by_group_abbr
    where
        metric_grouper = 'RN'

    union all

    select
        'UAPhours' as metric_abbreviation,
         metric_dt_key,
        cost_center_id,
        metric_grouper,
        direct_hours as numerator
    from
        direct_hours_by_group_abbr
    where
        metric_grouper = 'UAP'

    union all

    select
        'HPPDhrsSubtrahend' as metric_abbreviation,
        pp_end_dt_key as metric_dt_key,
        cost_center_id,
        metric_grouper,
        uap_safety_obs_hours as numerator
    from
         {{ ref('stg_nccs_hppd_p3_uap_safety_obs_hours') }}
    where
        metric_grouper = 'UAP'
)

select
    metric_abbreviation,
    metric_dt_key,
    cost_center_id,
    metric_grouper,
    numerator
from
    metric_row
