/* stg_nursing_staff_p8a_loa
Part 8 step a gets LOA (Leave of Absence) FTE data by pay period from position control
at the job rollup level. Includes any past and future pay periods if available
*/

with
get_loa_fte as (
    select
        position_control.pp_dt_key as metric_dt_key,
        position_control.cost_center_id,
        position_control.cost_center_site_id,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            case
                when get_job_group.fixed_rn_override_ind = 1
                then get_job_group.rn_alt_job_group_id
                else get_job_group.use_job_group_id
            end) as job_group_id,
        sum(case when position_control.tag = 'PC UNIT LOAs'
            then position_control.aggregated_value end) as loa_fte
    from {{ ref('nursing_position_control_period_unit_metric') }} as position_control
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as get_job_group
        on position_control.job_code = get_job_group.job_code
    left join {{ ref('job_group_levels_nursing') }} as job_group_levels_nursing
        on case
            when get_job_group.fixed_rn_override_ind = 1
            then get_job_group.rn_alt_job_group_id
            else get_job_group.use_job_group_id
        end = job_group_levels_nursing.job_group_id
    where
        position_control.tag = 'PC UNIT LOAs'
    group by
        position_control.pp_dt_key,
        position_control.cost_center_id,
        position_control.cost_center_site_id,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            case
                when get_job_group.fixed_rn_override_ind = 1
                then get_job_group.rn_alt_job_group_id
                else get_job_group.use_job_group_id
            end)
)

select
    'LOAfteJr' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    loa_fte as numerator,
    null::numeric as denominator,
    loa_fte as row_metric_calculation
from get_loa_fte
