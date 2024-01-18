{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_direct_p5_pp_nursing_ops
caputure the numerator and denominator components that make the metric
for the % of staffing for a pay period who came from nursing operations
personnel who were deployed to the various units:
numerator:  those from centralized staffing working in other cost centers
denominator: total productive direct hours for those same job groups
Also roll these up for the StaffNurse job groups
Note, for Nursing Operations (centralized staffing - 10900) since the percent metric needs all
regular and overtime time in the denominator, TIME_w4 can cover it so that
even if for the cost center/job group combo no centralized recources were
utilized a percent row is written, but here do a Staff Nurse (level 4 rollup) for both
the numerator and denominator
*/
with
worker_direct_hrs as (
    select
        pp_end_dt_key as metric_dt_key,
        cost_center_id,
        job_group_id,
        worker_id,
        future_pay_period_ind,
        staff_nurse_ind,
        productive_direct_pp_hours
    from
        {{ ref('stg_worker_direct_pp_hours') }}
),

central_staff_chk as (
    /* answers if the worker for this pay period is in the centralized cost center */
    select
        unit_metric.worker_id,
        unit_metric.pp_dt_key,
        cost_center.cost_center_id as nod_cost_center_id
        /* nod = nursing operations department which in part manages centralized
        staffing to hospital units for various job roles, mostly staff nurses */
    from
        {{ ref('nursing_position_control_period_unit_metric') }} as unit_metric
	inner join {{ ref('nursing_cost_center_attributes') }} as cost_center
            on unit_metric.cost_center_id
            = cost_center.cost_center_id
            and cost_center.cost_center_id = '10900' -- centralized staffing
    where
        unit_metric.tag = 'PC Hired HeadCount'
        and unit_metric.aggregated_value > 0
),

capture_nod_component as (
    select
        worker_direct_hrs.metric_dt_key,
        worker_direct_hrs.cost_center_id,
        worker_direct_hrs.job_group_id,
        worker_direct_hrs.worker_id,
        case
            when central_staff_chk.worker_id is null
            then 0
            else worker_direct_hrs.productive_direct_pp_hours
        end as nod_numerator,
        worker_direct_hrs.productive_direct_pp_hours as nod_denominator,
        worker_direct_hrs.future_pay_period_ind,
        worker_direct_hrs.staff_nurse_ind,
        central_staff_chk.nod_cost_center_id
    from
        worker_direct_hrs
        left join central_staff_chk
            on worker_direct_hrs.worker_id = central_staff_chk.worker_id
            and worker_direct_hrs.metric_dt_key = central_staff_chk.pp_dt_key
),

build_nod_component_numerator as (
    select
        /* numerator will sometimes be 0 but we need in order that all
        corresponding denominator data is included */
        case capture_nod_component.future_pay_period_ind
            when 1
            then 'Upcoming' else '' end
        || case capture_nod_component.cost_center_id
            when capture_nod_component.nod_cost_center_id then 'NODNODHrs'
            else 'DeployedNODHrs' end as metric_abbreviation,
        capture_nod_component.metric_dt_key,
        capture_nod_component.cost_center_id,
        capture_nod_component.job_group_id,
        capture_nod_component.staff_nurse_ind,
        sum(capture_nod_component.nod_numerator) as numerator
    from
        capture_nod_component
    group by
        capture_nod_component.metric_dt_key,
        capture_nod_component.cost_center_id,
        capture_nod_component.job_group_id,
        capture_nod_component.staff_nurse_ind,
        capture_nod_component.future_pay_period_ind,
        capture_nod_component.nod_cost_center_id
),

n_ops_metric_row_components as (
     select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        sum(numerator) as numerator
    from
        build_nod_component_numerator
    group by
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id

    union all

    select
        'sNurse' || metric_abbreviation as metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        sum(numerator) as numerator
    from
        build_nod_component_numerator
    where
        staff_nurse_ind = 1
    group by
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id

    union all

    select
        'sNurseDirectHrs' as metric_abbreviation,
        capture_nod_component.metric_dt_key,
        capture_nod_component.cost_center_id,
        capture_nod_component.job_group_id,
        sum(capture_nod_component.nod_denominator) as numerator
    from
        capture_nod_component
    where
        capture_nod_component.future_pay_period_ind = 0 /* only for the past get denom for % */
        and capture_nod_component.staff_nurse_ind = 1
    group by
        capture_nod_component.metric_dt_key,
        capture_nod_component.cost_center_id,
        capture_nod_component.job_group_id,
        capture_nod_component.future_pay_period_ind
)
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
    n_ops_metric_row_components
