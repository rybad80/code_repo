{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_w1a_hrs_direct
collect the regular and overtime total hours by job group,
and then subsets for Per Diem (UP2), Overtime, Ambulatory staff nurses
plus Nursing Operations (direct_p5), and Safety Obs (direct_p4)
*/

with
direct_subset_hours as (
    select
        prior_pay_period_ind,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        aggregate_pp_sum_hours,
        aggregate_overtime_hours,
        aggregate_per_diem_rn_hours,
        aggregate_ambulatory_rn_hours
    from
        {{ ref('stg_nursing_direct_p2_pp_subset') }}
),

ambulatory_rn_hours as (
    select
        'DirectHrsAmbRN' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        'AmbulatoryRN' as job_group_id,
        sum(aggregate_ambulatory_rn_hours) as direct_amb_hours
    from
        direct_subset_hours
    where
        prior_pay_period_ind = 1
    group by
        metric_dt_key,
        cost_center_id
),

metric_row_components as ( /* direct totals, per diem hrs, overtime hrs, ambulatory only hrs */

    select
        'DirectHrs' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        aggregate_pp_sum_hours as numerator
    from
        direct_subset_hours
    where
        prior_pay_period_ind = 1

    union all

    select
        case prior_pay_period_ind
            when 0 then 'Upcoming'
            else ''
            end
        || 'PerDiemHrs' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        aggregate_per_diem_rn_hours as numerator
    from
        direct_subset_hours
    where
        aggregate_per_diem_rn_hours > 0

    union all

    select
        case prior_pay_period_ind
            when 0 then 'Upcoming'
            else '' end
            || 'OvertimeHrs' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        aggregate_overtime_hours as numerator
    from
        direct_subset_hours
    where
        aggregate_overtime_hours > 0

    union all

    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        direct_amb_hours as numerator
    from
        ambulatory_rn_hours
    where
        direct_amb_hours > 0
)

/* Now add in also safety obs and nursing ops hours */
select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    job_group_id as metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    metric_row_components

union all

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    job_group_id as metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    {{ ref('stg_nursing_direct_p4_pp_safety_obs') }}

union all

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    job_group_id as metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    {{ ref('stg_nursing_direct_p5_pp_nursing_ops') }}
