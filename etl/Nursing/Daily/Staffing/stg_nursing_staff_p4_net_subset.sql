/* stg_nursing_staff_p4_net_subset
capture the workers' FTEs that are adding to NCCS units or transferring to other
cost centers or jobs in the future (per Workday recruiting data) and arrrange into
the subsets for reporting on staffing: net chgs, Add, Minus; and to also prep for rollups
to upcoming
*/
with
net_cc_job_fte as (
    select
        'UpcomingFTEjgDelta' as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        sum(date_cc_job_group_fte) as net_change
     from
        {{ ref('stg_nursing_staff_p3_cc_jgrp') }}
    group by
        metric_dt_key,
        cost_center_id,
        job_group_id
),

delta_fte_lvl4 as ( /* by the start date granularity and rollup */
    select
        'UpcomingStaffNetChgLvl4Start' as metric_abbreviation,
        net_cc_job_fte.metric_dt_key,
        net_cc_job_fte.cost_center_id,
        coalesce(
            lvls.nursing_job_rollup,
            net_cc_job_fte.job_group_id,
            'unk Job Grp ID') as job_group_id,
        sum(net_cc_job_fte.net_change) as numerator
    from
        net_cc_job_fte
        left join {{ ref('job_group_levels_nursing') }} as lvls
            on net_cc_job_fte.job_group_id = lvls.job_group_id
    group by
        net_cc_job_fte.metric_dt_key,
        net_cc_job_fte.cost_center_id,
        coalesce(lvls.nursing_job_rollup,
            net_cc_job_fte.job_group_id,
            'unk Job Grp ID')
)

/* additional FTE incoming for cost center due to position filled */
select
    metric_abbreviation, /* StaffWrkrAdd */
    metric_dt_key,
    worker_id,
    cost_center_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator as numerator
from
   {{ ref('stg_nursing_staff_p1_incoming_fte') }}

union all
/* incoming subset by internal transfer vs hire */
select
    case internal_ind
        when 1
        then 'StaffWrkrAddTransfer'
        else 'StaffWrkrAddHire'
    end as metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator as numerator
from
   {{ ref('stg_nursing_staff_p1_incoming_fte') }}

union all
/* loss for cost center due to a transfer or job change */
/* ultimately needs to include future termainations also */
select
    metric_abbreviation, /* StaffWrkrMinus */
    metric_dt_key,
    worker_id,
    cost_center_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator
from
    {{ ref('stg_nursing_staff_p2_outgoing_fte') }}

union all
/* incoming subset by internal transfer (vs termination from CHOP to be available later) */
select
    'StaffWrkrMinusTransfer' as metric_abbreviation,
    metric_dt_key,
    worker_id,
    cost_center_id,
    job_code,
    job_group_id,
    metric_grouper,
    numerator
from
    {{ ref('stg_nursing_staff_p2_outgoing_fte') }}
where
    internal_ind = 1

union all
  select
    metric_abbreviation, /* UpcomingFTEjgDelta -> UpcomingStaffNetChgLvl4Start */
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    numerator
from
    delta_fte_lvl4
