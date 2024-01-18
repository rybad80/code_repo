/* stg_nursing_staff_p6_vacancy_component */

with
loa_and_orient as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        loa_fte_next_3_pp,
        orient_fte_next_3_pp
    from
        {{ ref('stg_nursing_staff_p5_func_component') }}
),

current_fte as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        row_metric_calculation as curr_fte
    from
        {{ ref('stg_nursing_staff_w1_current_fte') }}
    where
        metric_abbreviation = 'currFTElvl4'
),

upcoming_net_change as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        row_metric_calculation as net_change_next_3_pp
    from
        {{ ref('stg_nursing_staff_w5_upcoming') }}
    where
        metric_abbreviation = 'StaffNetChg3pp'
),

upcoming_fte as (
    select
        coalesce(
            current_fte.metric_dt_key,
            loa_and_orient.metric_dt_key,
            upcoming_net_change.metric_dt_key
            ) as metric_dt_key,
        coalesce(
            current_fte.cost_center_id,
            loa_and_orient.cost_center_id,
            upcoming_net_change.cost_center_id
            ) as cost_center_id,
        coalesce(
            current_fte.job_group_id,
            loa_and_orient.job_group_id,
            upcoming_net_change.job_group_id
            ) as job_group_id,
        coalesce(current_fte.curr_fte, 0) as curr_fte,
        coalesce(loa_and_orient.loa_fte_next_3_pp, 0) as loa_fte_next_3_pp,
        coalesce(loa_and_orient.orient_fte_next_3_pp, 0) as orient_fte_next_3_pp,
        coalesce(upcoming_net_change.net_change_next_3_pp, 0) as net_change_next_3_pp,
        coalesce(current_fte.curr_fte, 0)
            + coalesce(upcoming_net_change.net_change_next_3_pp, 0) as upcoming_fte_next_3_pp,
        coalesce(current_fte.curr_fte, 0)
            - coalesce(loa_and_orient.loa_fte_next_3_pp, 0)
            - coalesce(loa_and_orient.orient_fte_next_3_pp, 0)
            + coalesce(upcoming_net_change.net_change_next_3_pp, 0) as func_fte_next_3_pp
    from
        current_fte
    full outer join loa_and_orient
        on current_fte.metric_dt_key = loa_and_orient.metric_dt_key
        and current_fte.cost_center_id = loa_and_orient.cost_center_id
        and current_fte.job_group_id = loa_and_orient.job_group_id
    full outer join upcoming_net_change
        on current_fte.metric_dt_key = upcoming_net_change.metric_dt_key
        and current_fte.cost_center_id = upcoming_net_change.cost_center_id
        and current_fte.job_group_id = upcoming_net_change.job_group_id
),

upcoming_vac as (
    select
        coalesce(
            vacancy.metric_dt_key,
            loa_and_orient.metric_dt_key,
            upcoming_net_change.metric_dt_key
            ) as metric_dt_key,
        coalesce(
            vacancy.cost_center_id,
            loa_and_orient.cost_center_id,
            upcoming_net_change.cost_center_id
            ) as cost_center_id,
        coalesce(
            vacancy.job_group_id,
            loa_and_orient.job_group_id,
            upcoming_net_change.job_group_id
            ) as job_group_id,
        coalesce(vacancy.numerator, 0)
            - coalesce(upcoming_net_change.net_change_next_3_pp, 0)
            as upcoming_vacancy_fte_next_3_pp,
        coalesce(vacancy.numerator, 0)
            + coalesce(loa_and_orient.loa_fte_next_3_pp, 0)
            + coalesce(loa_and_orient.orient_fte_next_3_pp, 0)
            - coalesce(upcoming_net_change.net_change_next_3_pp, 0)
            as func_vacancy_fte_next_3_pp,
        coalesce(vacancy.denominator, 0) as budget,
        case when budget = 0
            then null
            else upcoming_vacancy_fte_next_3_pp / budget
            end as upcoming_vac_rate_next_3_pp,
        case when budget = 0
            then null
            else func_vacancy_fte_next_3_pp / budget
            end as func_vac_rate_next_3_pp
    from
        {{ ref('stg_nursing_staff_w4_vacancy') }} as vacancy
    full outer join loa_and_orient
        on vacancy.metric_dt_key = loa_and_orient.metric_dt_key
        and vacancy.cost_center_id = loa_and_orient.cost_center_id
        and vacancy.job_group_id = loa_and_orient.job_group_id
    full outer join upcoming_net_change
        on vacancy.metric_dt_key = upcoming_net_change.metric_dt_key
        and vacancy.cost_center_id = upcoming_net_change.cost_center_id
        and vacancy.job_group_id = upcoming_net_change.job_group_id
    where
        vacancy.metric_abbreviation = 'JobGrp4VacancyRate'
)

select
    coalesce(upcoming_fte.metric_dt_key, upcoming_vac.metric_dt_key) as metric_dt_key,
    coalesce(upcoming_fte.cost_center_id, upcoming_vac.cost_center_id) as cost_center_id,
    coalesce(upcoming_fte.job_group_id, upcoming_vac.job_group_id) as job_group_id,
    coalesce(upcoming_fte.upcoming_fte_next_3_pp, 0) as upcoming_fte_next_3_pp,
    coalesce(upcoming_fte.func_fte_next_3_pp, 0) as func_fte_next_3_pp,
    coalesce(upcoming_vac.upcoming_vacancy_fte_next_3_pp, 0) as upcoming_vacancy_fte_next_3_pp,
    coalesce(upcoming_vac.func_vacancy_fte_next_3_pp, 0) as func_vacancy_fte_next_3_pp,
    coalesce(upcoming_vac.budget, 0) as budget,
    coalesce(upcoming_vac.upcoming_vac_rate_next_3_pp, 0) as upcoming_vac_rate_next_3_pp,
    coalesce(upcoming_vac.func_vac_rate_next_3_pp, 0) as func_vac_rate_next_3_pp
from upcoming_fte
full outer join upcoming_vac
    on upcoming_fte.metric_dt_key = upcoming_vac.metric_dt_key
    and upcoming_fte.cost_center_id = upcoming_vac.cost_center_id
    and upcoming_fte.job_group_id = upcoming_vac.job_group_id
