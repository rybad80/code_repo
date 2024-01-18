/* stg_nursing_staff_w4_vacancy
 vacancy is the budgeted FTE minus the actual current FTE
 and vacancy rate is that difference from the budget divided
 by the budget
 And only show vacancy for cost centers for which we have budgets
 */
with
curr_fte as (
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id::varchar(5) as cost_center_id,
        job_group_id,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_staff_w1_current_fte') }}
    where
        metric_abbreviation = 'currFTElvl4'
),

adj_budget as (
    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id::varchar(5) as cost_center_id,
        job_group_id,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_staff_w3_adjust_budget') }}
    where
        metric_abbreviation = 'currFTEBdgtLvl4Adj'
),

components_for_vacancy as (
    select
        'JobGrp4VacancyFTE' as metric_abbreviation,
        coalesce(curr_fte.metric_dt_key, adj_budget.metric_dt_key ) as metric_dt_key,
        coalesce(curr_fte.cost_center_id, adj_budget.cost_center_id) as cost_center_id,
        coalesce(curr_fte.job_group_id, adj_budget.job_group_id ) as job_group_id,
        coalesce(adj_budget.row_metric_calculation, 0) as numerator,
        coalesce(curr_fte.numerator, 0) as denominator,
        coalesce(adj_budget.row_metric_calculation, 0)
        - coalesce(curr_fte.numerator, 0) as row_metric_calculation
    from
        curr_fte
        full outer join adj_budget
            on curr_fte.job_group_id = adj_budget.job_group_id
            and curr_fte.metric_dt_key = adj_budget.metric_dt_key
            and curr_fte.cost_center_id = adj_budget.cost_center_id
),

fte_vacancy_metric_rows as (
    select
        vac.metric_abbreviation,
        vac.metric_dt_key,
        vac.cost_center_id,
        vac.job_group_id,
        round(vac.row_metric_calculation, 2) as numerator,
        null::numeric as denominator,
        round(vac.row_metric_calculation, 2) as row_metric_calculation
    from
        components_for_vacancy as vac
        inner join {{ ref('nursing_cost_center_attributes') }} as cc
            on vac.cost_center_id = cc.cost_center_id
            and cc.has_nursing_current_year_budget_ind = 1
),

vacancy_rate as (
    select
        'JobGrp4VacancyRate' as metric_abbreviation,
        vac.metric_dt_key,
        vac.cost_center_id,
        vac.job_group_id,
        vac.row_metric_calculation as numerator,
        vac.numerator as denominator,
        case
            when vac.numerator = 0 then null
            else round(vac.row_metric_calculation / vac.numerator, 3)
        end as row_metric_calculation
    from
        components_for_vacancy as vac
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
    fte_vacancy_metric_rows

union all

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    null as metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
    vacancy_rate
