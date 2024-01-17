/* stg_nursing_staff_w6_functional
enhance the staffing vacancy by subtracting out associated
LOA (leave of absence) and orientation from the staff FTEs
and recalculating the variance from budget and functional
staff vacancy as this is a more  precise measure of the staffing
scenarios nursing management has to practically remediate
for patient care to be covered
*/

with
loa_and_orient as (
    select
        metric_dt_key,
        cost_center_id,
        job_group_id,
        loa_fte,
        loa_fte_next_3_pp,
        orient_fte,
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

functional_fte as (
    select
        coalesce(
            current_fte.metric_dt_key,
            loa_and_orient.metric_dt_key) as metric_dt_key,
        coalesce(
            current_fte.cost_center_id,
            loa_and_orient.cost_center_id) as cost_center_id,
        coalesce(
            current_fte.job_group_id,
            loa_and_orient.job_group_id) as job_group_id,
        coalesce(current_fte.curr_fte, 0) as curr_fte,
        coalesce(loa_and_orient.loa_fte, 0) as loa_fte,
        coalesce(loa_and_orient.orient_fte, 0) as orient_fte,
        coalesce(current_fte.curr_fte, 0)
            - coalesce(loa_and_orient.loa_fte, 0)
            - coalesce(loa_and_orient.orient_fte, 0) as func_fte
    from
        current_fte
    full outer join loa_and_orient
        on current_fte.metric_dt_key = loa_and_orient.metric_dt_key
        and current_fte.cost_center_id = loa_and_orient.cost_center_id
        and current_fte.job_group_id = loa_and_orient.job_group_id
),

func_vacancy as (
    select
        coalesce(
            vacancy.metric_dt_key,
            loa_and_orient.metric_dt_key) as metric_dt_key,
        coalesce(
            vacancy.cost_center_id,
            loa_and_orient.cost_center_id) as cost_center_id,
        coalesce(
            vacancy.job_group_id,
            loa_and_orient.job_group_id) as job_group_id,
        coalesce(vacancy.numerator, 0)
            + coalesce(loa_and_orient.loa_fte, 0)
            + coalesce(loa_and_orient.orient_fte, 0) as func_vacancy_fte,
        coalesce(vacancy.numerator, 0)
            + coalesce(loa_and_orient.loa_fte_next_3_pp, 0)
            + coalesce(loa_and_orient.orient_fte_next_3_pp, 0) as func_vacancy_fte_next_3_pp,
        coalesce(vacancy.denominator, 0) as budget,
        case when budget = 0
            then null
            else func_vacancy_fte / budget
            end as func_vac_rate
    from
        {{ ref('stg_nursing_staff_w4_vacancy') }} as vacancy
    full outer join loa_and_orient
        on vacancy.metric_dt_key = loa_and_orient.metric_dt_key
        and vacancy.cost_center_id = loa_and_orient.cost_center_id
        and vacancy.job_group_id = loa_and_orient.job_group_id
    where
        vacancy.metric_abbreviation = 'JobGrp4VacancyRate'
)

select
    'FunctionalFTE' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    func_fte as numerator,
    null::numeric as denominator,
    func_fte as row_metric_calculation
from
    functional_fte

union all

select
    'FuncVacancyFTE' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    func_vacancy_fte as numerator,
    null::numeric as denominator,
    func_vacancy_fte as row_metric_calculation
from
    func_vacancy

union all

select
    'FuncVacancyRate' as metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    func_vacancy_fte as numerator,
    budget as denominator,
    func_vac_rate as row_metric_calculation
from
    func_vacancy
