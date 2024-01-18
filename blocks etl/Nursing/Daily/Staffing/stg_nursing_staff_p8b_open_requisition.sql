/* stg_nursing_staff_p8b_open_requisition
Part 8 step b gets open requisition data from Workday for nursing cost centers by VP,
includes aggregated rows by job group rollup and detailed rows
by job code */

with
curr_pp as (
    select
        pp_end_dt_key as metric_dt_key,
        pp_start_dt,
        pp_end_dt
    from
        {{ ref('nursing_pay_period') }}
    where
        current_working_pay_period_ind = 1
),

nursing_cc as (
    select
        cost_center_id
    from {{ ref('nursing_cost_center_attributes') }}
    where has_nursing_current_year_budget_ind = 1
),

open_reqs_rollup as (
    select
        curr_pp.metric_dt_key,
        substring(cr_workday_job_requisitions.costcenter, 1, 5) as cost_center_id,
        cr_workday_job_requisitions.vplastname as metric_grouper,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            case
                when stg_nursing_job_code_group_statistic.fixed_rn_override_ind = 1
                then stg_nursing_job_code_group_statistic.rn_alt_job_group_id
                else stg_nursing_job_code_group_statistic.use_job_group_id
            end) as job_group_rollup,
        count(cr_workday_job_requisitions.jobrecid) as open_reqs
    from {{ source('workday_ods', 'cr_workday_job_requisitions') }} as cr_workday_job_requisitions
    inner join nursing_cc
        on substring(cr_workday_job_requisitions.costcenter, 1, 5) = nursing_cc.cost_center_id
    left join curr_pp
        on date(cr_workday_job_requisitions.approveddate) + cast(cr_workday_job_requisitions.days_open as integer)
        between curr_pp.pp_start_dt and curr_pp.pp_end_dt
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as stg_nursing_job_code_group_statistic
        on cr_workday_job_requisitions.jobcode = stg_nursing_job_code_group_statistic.job_code
    left join {{ ref('job_group_levels_nursing') }} as job_group_levels_nursing
        on case
            when stg_nursing_job_code_group_statistic.fixed_rn_override_ind = 1
            then stg_nursing_job_code_group_statistic.rn_alt_job_group_id
            else stg_nursing_job_code_group_statistic.use_job_group_id
        end = job_group_levels_nursing.job_group_id
    where lower(requisitionstatus) = 'open'
    group by
        curr_pp.metric_dt_key,
        cr_workday_job_requisitions.costcenter,
        cr_workday_job_requisitions.vplastname,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            case
                when stg_nursing_job_code_group_statistic.fixed_rn_override_ind = 1
                then stg_nursing_job_code_group_statistic.rn_alt_job_group_id
                else stg_nursing_job_code_group_statistic.use_job_group_id
        end)
),

open_reqs_detail as (
    select
        case cr_workday_job_requisitions.employmentstatus
            when 'Regular - Full time' then 'RegFT'
            when 'Regular - Part time' then 'RegPT'
            when 'Temporary - Full time' then 'TempFT'
            when 'Temporary - Part time' then 'TempPT'
        end as emp_status,
        curr_pp.metric_dt_key,
        cr_workday_job_requisitions.jobcode as job_code,
        substring(cr_workday_job_requisitions.costcenter, 1, 5) as cost_center_id,
        cr_workday_job_requisitions.vplastname as metric_grouper,
        stg_nursing_job_code_group_statistic.use_job_group_id as job_group_id,
        count(cr_workday_job_requisitions.jobrecid) as open_reqs
    from {{ source('workday_ods', 'cr_workday_job_requisitions') }} as cr_workday_job_requisitions
    inner join nursing_cc
        on substring(cr_workday_job_requisitions.costcenter, 1, 5) = nursing_cc.cost_center_id
    left join curr_pp
        on date(cr_workday_job_requisitions.approveddate) + cast(cr_workday_job_requisitions.days_open as integer)
        between curr_pp.pp_start_dt and curr_pp.pp_end_dt
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as stg_nursing_job_code_group_statistic
            on cr_workday_job_requisitions.jobcode = stg_nursing_job_code_group_statistic.job_code
    where lower(cr_workday_job_requisitions.requisitionstatus) = 'open'
    group by
        curr_pp.metric_dt_key,
        cr_workday_job_requisitions.jobcode,
        cr_workday_job_requisitions.employmentstatus,
        cr_workday_job_requisitions.costcenter,
        cr_workday_job_requisitions.vplastname,
        stg_nursing_job_code_group_statistic.use_job_group_id
)

select
    'OpenReqsJr' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_rollup as job_group_id,
    metric_grouper,
    open_reqs as numerator,
    null::numeric as denominator,
    open_reqs as row_metric_calculation
from open_reqs_rollup

union all

select
    'OpenReqsDtl' || emp_status as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    job_code,
    job_group_id,
    metric_grouper,
    open_reqs as numerator,
    null::numeric as denominator,
    open_reqs as row_metric_calculation
from open_reqs_detail
