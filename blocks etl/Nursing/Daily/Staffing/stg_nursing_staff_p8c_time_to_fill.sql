/* stg_nursing_staff_p8c_time_to_fill
Part 8 step c calculates average time to fill in days for positions in nursing
cost centers, also gathers detailed rows by individual position */

with
nursing_cc as (
    select
        cost_center_id
    from {{ ref('nursing_cost_center_attributes') }}
    where has_nursing_current_year_budget_ind = 1
),

workday_time_to_fill as (
    select
        cr_workday_neo.application_id,
        case
            when cast(cr_workday_neo.hired_on as date) is null
                then current_date
            else cast(cr_workday_neo.hired_on as date)
        end as hire_on,
        nursing_pay_period.pp_end_dt_key as metric_dt_key,
        nursing_cc.cost_center_id,
        cr_workday_neo.job_code,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            case
                when stg_nursing_job_code_group_statistic.fixed_rn_override_ind = 1
                then stg_nursing_job_code_group_statistic.rn_alt_job_group_id
                else stg_nursing_job_code_group_statistic.use_job_group_id
            end) as job_group_rollup,
        stg_nursing_job_code_group_statistic.use_job_group_id as job_group_id,
        cr_workday_neo.vp_name,
        cast(cr_workday_neo.time_to_fill as integer) as time_to_fill
    from {{ source('workday_ods', 'cr_workday_neo') }} as cr_workday_neo
    inner join nursing_cc
        on substring(cr_workday_neo.cost_center, 1, 5) = nursing_cc.cost_center_id
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as stg_nursing_job_code_group_statistic
        on cr_workday_neo.job_code = stg_nursing_job_code_group_statistic.job_code
    left join {{ ref('job_group_levels_nursing') }} as job_group_levels_nursing
        on case
            when stg_nursing_job_code_group_statistic.fixed_rn_override_ind = 1
            then stg_nursing_job_code_group_statistic.rn_alt_job_group_id
            else stg_nursing_job_code_group_statistic.use_job_group_id
        end = job_group_levels_nursing.job_group_id
    left join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on case
            when cast(cr_workday_neo.hired_on as date) is null
                then current_date
            else cast(cr_workday_neo.hired_on as date) end
        between nursing_pay_period.pp_start_dt and nursing_pay_period.pp_end_dt
)

select
    'DaysToFillJrAvg' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_rollup as job_group_id,
    vp_name as metric_grouper,
    sum(time_to_fill) as numerator,
    count(application_id) as denominator,
    round(sum(time_to_fill) / count(application_id), 2) as row_metric_calculation
from workday_time_to_fill
group by
    metric_dt_key,
    cost_center_id,
    job_group_rollup,
    vp_name

union all

select
    'DaysToFillDtl' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    job_code,
    job_group_id,
    vp_name as metric_grouper,
    time_to_fill as numerator,
    null::numeric as denominator,
    time_to_fill as row_metric_calculation
from workday_time_to_fill
