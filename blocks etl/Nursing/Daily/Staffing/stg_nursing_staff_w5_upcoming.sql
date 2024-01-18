/* stg_nursing_staff_w5_upcoming
capture the workers' FTEs that are adding to NCCS units or transferring to other
cost centers or jobs in the future (per Workday recruiting data)
*/
with
union_delta_type as (
select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator as subset_fte
from
   {{ ref('stg_nursing_staff_p4_net_subset') }}
)

/* carry forward the various delta subsets */
select
    metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    null as cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    subset_fte as numerator,
    null::numeric as denominator,
    subset_fte as row_metric_calculation
from
    union_delta_type

union all
/* summarize for Adds and minus metrics to upcoming by cost center/job rollup - for Qlik Staffing */
select
    replace(delta_fte_jg.metric_abbreviation,
        'StaffWrkr',
        'UpcomingStaff') as metric_abbreviation,
    current_pp.pp_end_dt_key as metric_dt_key,
    null as worker_id,
    delta_fte_jg.cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    delta_fte_jg.job_group_id,
    null as metric_grouper,
    sum(delta_fte_jg.subset_fte) as numerator,
    null::numeric as denominator,
    sum(delta_fte_jg.subset_fte) as row_metric_calculation
from
    union_delta_type as delta_fte_jg
    inner join {{ ref('nursing_pay_period') }} as current_pp
        on current_pp.current_working_pay_period_ind = 1
where
    delta_fte_jg.metric_abbreviation in (
        'StaffWrkrAdd',
        'StaffWrkrAddHire',
        'StaffWrkrAddTransfer',
        'StaffWrkrMinus',
        'StaffWrkrMinusTransfer',
        'StaffWrkrMinusTerm'
        )
group by
    delta_fte_jg.metric_abbreviation,
    current_pp.pp_end_dt_key,
    delta_fte_jg.cost_center_id,
    delta_fte_jg.job_group_id

union all
/* summarize for any upcoming by cost center/job rollup - for Qlik Staffing */
select
    'UpcomingStaffNetChgLvl4' as metric_abbreviation,
    current_pp.pp_end_dt_key as metric_dt_key,
    null as worker_id,
    delta_fte_lvl4.cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    delta_fte_lvl4.job_group_id,
    null as metric_grouper,
    sum(delta_fte_lvl4.subset_fte) as numerator,
    null::numeric as denominator,
    sum(delta_fte_lvl4.subset_fte) as row_metric_calculation
from
    union_delta_type as delta_fte_lvl4
    inner join {{ ref('nursing_pay_period') }} as current_pp
        on current_pp.current_working_pay_period_ind = 1
where
    metric_abbreviation = 'UpcomingStaffNetChgLvl4Start'
group by
    current_pp.pp_end_dt_key,
    delta_fte_lvl4.cost_center_id,
    delta_fte_lvl4.job_group_id

union all
/* summarize for those in window of next three pay periods */
select
    'StaffNetChg3pp' as metric_abbreviation,
    current_pp.pp_end_dt_key as metric_dt_key,
    null as worker_id,
    delta_fte_lvl4.cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    delta_fte_lvl4.job_group_id,
    null as metric_grouper,
    sum(delta_fte_lvl4.subset_fte) as numerator,
    null::numeric as denominator,
    sum(delta_fte_lvl4.subset_fte) as row_metric_calculation
from
    union_delta_type as delta_fte_lvl4
    inner join {{ ref('nursing_pay_period') }}  as next_3_pp
        on delta_fte_lvl4.metric_dt_key
            between next_3_pp.pp_start_dt_key and next_3_pp.pp_end_dt_key
            and next_3_pp.upcoming_3_pay_periods_ind = 1
    inner join {{ ref('nursing_pay_period') }} as current_pp
        on current_pp.current_working_pay_period_ind = 1
where
    metric_abbreviation = 'UpcomingStaffNetChgLvl4Start'
group by
    current_pp.pp_end_dt_key,
    delta_fte_lvl4.cost_center_id,
    delta_fte_lvl4.job_group_id
