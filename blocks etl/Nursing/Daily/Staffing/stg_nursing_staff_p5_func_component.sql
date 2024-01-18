/* stg_nursing_staff_p5_func_component
gather components for the staffing functional vacancy:
LOA (leave of absence) - from Position Control (best we have)
and Orientation - based on Kronos
*/

with
use_pp as (
select
    pp_end_dt,
    pp_end_dt_key,
    to_char(pp_end_dt + 14, 'yyyymmdd')::bigint as current_pay_period,
    latest_pay_period_ind,
    null as third_next_pp_ind
from {{ ref('nursing_pay_period') }}
where
    latest_pay_period_ind = 1

union all

select
    pp_end_dt + 42 as third_next_pp_end_dt,
    to_char(third_next_pp_end_dt, 'yyyymmdd')::bigint as pp_end_dt_key,
    to_char(pp_end_dt + 14, 'yyyymmdd')::bigint as current_pay_period,
    null as latest_pay_period_ind,
    1 as third_next_pp_ind
from {{ ref('nursing_pay_period') }}
where
    latest_pay_period_ind = 1
),

lvl_4_loa_fte as (
    select
        use_pp.current_pay_period as metric_dt_key,
        position_control.cost_center_id,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            case
                when get_job_group.fixed_rn_override_ind = 1
                then get_job_group.rn_alt_job_group_id
                else get_job_group.use_job_group_id
            end) as job_group_id,
        sum(case when position_control.tag = 'PC UNIT LOAs'
            and use_pp.latest_pay_period_ind = 1
            then position_control.aggregated_value end) as loa_fte,
        sum(case when position_control.tag = 'PC UNIT LOAs'
            and use_pp.third_next_pp_ind = 1
            then position_control.aggregated_value end) as loa_fte_next_3_pp,
        sum(case when position_control.tag = 'PC Orientation FTEs'
            and use_pp.third_next_pp_ind = 1
            then position_control.aggregated_value end) as orient_fte_next_3_pp
    from {{ ref('nursing_position_control_period_unit_metric') }} as position_control
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as get_job_group
        on position_control.job_code = get_job_group.job_code
    left join {{ ref('job_group_levels_nursing') }} as job_group_levels_nursing
        on case
            when get_job_group.fixed_rn_override_ind = 1
            then get_job_group.rn_alt_job_group_id
            else get_job_group.use_job_group_id
        end = job_group_levels_nursing.job_group_id
    inner join use_pp
        on position_control.pp_dt_key = use_pp.pp_end_dt_key
        and (use_pp.latest_pay_period_ind = 1
            or use_pp.third_next_pp_ind = 1)
    where
        position_control.tag in ('PC UNIT LOAs', 'PC Orientation FTEs')
    group by
        use_pp.current_pay_period,
        position_control.cost_center_id,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            case
                when get_job_group.fixed_rn_override_ind = 1
                then get_job_group.rn_alt_job_group_id
                else get_job_group.use_job_group_id
            end)
),

lvl_4_orient_fte as (
    select
        use_pp.current_pay_period as metric_dt_key,
        stg_nursing_time_w3_fte.cost_center_id,
        stg_nursing_time_w3_fte.cost_center_site_id,
        coalesce(
            job_group_levels_nursing.nursing_job_rollup,
            stg_nursing_time_w3_fte.job_group_id) as job_group_id,
        sum(stg_nursing_time_w3_fte.row_metric_calculation) as orient_fte
    from {{ ref('stg_nursing_time_w3_fte') }} as stg_nursing_time_w3_fte
    left join {{ ref('job_group_levels_nursing') }} as job_group_levels_nursing
        on stg_nursing_time_w3_fte.job_group_id = job_group_levels_nursing.job_group_id
    inner join use_pp
        on stg_nursing_time_w3_fte.metric_dt_key = use_pp.pp_end_dt_key
        and use_pp.latest_pay_period_ind = 1
    where
        stg_nursing_time_w3_fte.metric_abbreviation = 'OrientFTE'
    group by
        use_pp.current_pay_period,
        stg_nursing_time_w3_fte.cost_center_id,
        stg_nursing_time_w3_fte.cost_center_site_id,
        coalesce(job_group_levels_nursing.nursing_job_rollup, stg_nursing_time_w3_fte.job_group_id)
)

select
    coalesce(lvl_4_loa_fte.metric_dt_key, lvl_4_orient_fte.metric_dt_key) as metric_dt_key,
    coalesce(lvl_4_loa_fte.cost_center_id, lvl_4_orient_fte.cost_center_id) as cost_center_id,
--    coalesce(lvl_4_loa_fte.cost_center_site_id, ) as cost_center_site_id,
    coalesce(lvl_4_loa_fte.job_group_id, lvl_4_orient_fte.job_group_id) as job_group_id,
    coalesce(lvl_4_loa_fte.loa_fte, 0) as loa_fte,
    coalesce(lvl_4_loa_fte.loa_fte_next_3_pp, 0) as loa_fte_next_3_pp,
    coalesce(lvl_4_orient_fte.orient_fte, 0) as orient_fte,
    coalesce(lvl_4_loa_fte.orient_fte_next_3_pp, 0) as orient_fte_next_3_pp
from lvl_4_loa_fte
full outer join lvl_4_orient_fte
    on lvl_4_loa_fte.metric_dt_key = lvl_4_orient_fte.metric_dt_key
    and lvl_4_loa_fte.cost_center_id = lvl_4_orient_fte.cost_center_id
    and lvl_4_loa_fte.job_group_id = lvl_4_orient_fte.job_group_id
