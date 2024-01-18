{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_non_direct_p2_pp_subset
capture the subset of staff nurse hours for the productive indirect and time off
time roll-ups to cost center and jo group grnaularity for each cost center
*/
    select
        'sNurseNonDirectHrs' as metric_abbreviation,
        pp_end_dt_key,
        cost_center_id,
        job_group_id,
        null as metric_grouper,
        sum(non_direct_hours) as numerator
    from
         {{ ref('stg_nursing_non_direct_p1_pp_hours') }}
    where
        prior_pay_period_ind = 1 /* only for the past get denom for % */
        and staff_nurse_ind = 1
    group by
        pp_end_dt_key,
        cost_center_id,
        job_group_id,
        prior_pay_period_ind
