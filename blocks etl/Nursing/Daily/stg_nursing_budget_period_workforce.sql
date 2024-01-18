{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_budget_period_workforce
from the manually provided list from finance of grouped by type of time
budgets per job code, for pay periods:
roll-up the budgets (as needed by the TIME metrics)
pulling out orientation and the psychTech (behavioral health clinician)
budgets individually, summing cost center total budgets, and
breaking out the totals by job_group_id and job_code into the
nursing metric structure
*/
with lookup_nursing_staffing_annual_budget as (
    select
        operations_budget_department,
        job_title_display,
        budget_paycode_grouper,
        vp_name_at_budget_time,
        annual_fte_budget,
        company_id,
        cost_center_id,
        cost_center_site_id,
        cost_center_concat,
        job_code,
        job_description_budget_time,
        fiscal_year
    from
        {{ ref('lookup_nursing_staffing_annual_budget') }}
),

orientation_only_by_job_code as (
    select
        cost_center_id,
        cost_center_site_id,
        fiscal_year,
        round(sum(annual_fte_budget), 2) as orientation_fte_budget,
        job_code
    from lookup_nursing_staffing_annual_budget
    where budget_paycode_grouper = 'Orientation'
    group by
        cost_center_id,
        cost_center_site_id,
        fiscal_year,
        job_code
),

safety_obs_budget_job_codes as (
    select
        job_group_id as safety_observer_rollup,
        job_code
    from {{ ref('job_code_job_group_link') }}
    where job_group_id = 'SafetyObserver'
),

finance_budgets as (
    select
        case budget_paycode_grouper
            when 'Orientation' then 'OrientTotBdgt'
            when 'Overtime' then 'OvertimeTotBdgt'
            when 'Paid Personal Leave' then 'PplTotBdgt'
            when 'Regular' then 'RegTotBdgt'
            when 'Other FTE' then 'OtherTotBdgt'
            else null
        end as metric_abbreviation,
        cost_center_id,
        cost_center_site_id,
        fiscal_year,
        round(sum(annual_fte_budget), 2) as numerator,
        null as job_code,
        null as job_group_id,
        null as metric_grouper
    from lookup_nursing_staffing_annual_budget
    where budget_paycode_grouper is not null
    group by
        metric_abbreviation,
        cost_center_id,
        cost_center_site_id,
        fiscal_year

    union all

    select
        case lookup_nursing_staffing_annual_budget.budget_paycode_grouper
            when 'Orientation' then 'OrientJgBdgt'
            when 'Overtime' then 'OvertimeJgBdgt'
            when 'Paid Personal Leave' then 'PplJgBdgt'
            when 'Regular' then 'RegJgBdgt'
            when 'Other FTE' then 'OtherJgBdgt'
            when 'Other FTE - Parental Leave' then 'ParLvJgBdgt'
            else null
        end as metric_abbreviation,
        lookup_nursing_staffing_annual_budget.cost_center_id,
        lookup_nursing_staffing_annual_budget.cost_center_site_id,
        lookup_nursing_staffing_annual_budget.fiscal_year,
        round(sum(lookup_nursing_staffing_annual_budget.annual_fte_budget), 2) as numerator,
        null as job_code,
        stg_nursing_job_code_group.nursing_job_grouper as job_group_id,
        stg_nursing_job_code_group.nursing_job_grouper as metric_grouper
    from lookup_nursing_staffing_annual_budget as lookup_nursing_staffing_annual_budget
    inner join {{ ref('stg_nursing_job_code_group') }} as stg_nursing_job_code_group
        on lookup_nursing_staffing_annual_budget.job_code = stg_nursing_job_code_group.job_code
    where budget_paycode_grouper is not null
    group by
        metric_abbreviation,
        stg_nursing_job_code_group.nursing_job_grouper,
        lookup_nursing_staffing_annual_budget.cost_center_id,
        lookup_nursing_staffing_annual_budget.cost_center_site_id,
        lookup_nursing_staffing_annual_budget.fiscal_year

    union all

    select
        case lookup_nursing_staffing_annual_budget.budget_paycode_grouper
            when 'Orientation' then 'OrientJobCodeBdgt'
            when 'Overtime' then 'OvertimeJobCodeBdgt'
            when 'Paid Personal Leave' then 'PplJobCodeBdgt'
            when 'Regular' then 'RegJobCodeBdgt'
            when 'Other FTE' then 'OtherJobCodeBdgt'
            when 'Other FTE - Parental Leave' then 'ParLvJobCodeBdgt'
            else null
        end as metric_abbreviation,
        lookup_nursing_staffing_annual_budget.cost_center_id,
        lookup_nursing_staffing_annual_budget.cost_center_site_id,
        lookup_nursing_staffing_annual_budget.fiscal_year,
        round(sum(lookup_nursing_staffing_annual_budget.annual_fte_budget), 2) as numerator,
        stg_nursing_job_code_group.job_code,
        stg_nursing_job_code_group.nursing_job_grouper as job_group_id,
        stg_nursing_job_code_group.nursing_job_grouper as metric_grouper
    from lookup_nursing_staffing_annual_budget as lookup_nursing_staffing_annual_budget
    inner join {{ ref('stg_nursing_job_code_group') }} as stg_nursing_job_code_group
        on lookup_nursing_staffing_annual_budget.job_code = stg_nursing_job_code_group.job_code
    where budget_paycode_grouper is not null
    group by
        metric_abbreviation,
        stg_nursing_job_code_group.nursing_job_grouper,
        lookup_nursing_staffing_annual_budget.cost_center_id,
        lookup_nursing_staffing_annual_budget.cost_center_site_id,
        lookup_nursing_staffing_annual_budget.fiscal_year,
        stg_nursing_job_code_group.job_code

    union all

	select
        'SafetyObsBdgt' as metric_abbreviation,
        lookup_nursing_staffing_annual_budget.cost_center_id,
        lookup_nursing_staffing_annual_budget.cost_center_site_id,
        lookup_nursing_staffing_annual_budget.fiscal_year,
        round(sum(lookup_nursing_staffing_annual_budget.annual_fte_budget), 2) as numerator,
        null as job_code,
        safety_obs_budget_job_codes.safety_observer_rollup as job_group_id,
        safety_obs_budget_job_codes.safety_observer_rollup as metric_grouper
    from {{ ref('lookup_nursing_staffing_annual_budget') }} as lookup_nursing_staffing_annual_budget
    inner join safety_obs_budget_job_codes
        on lookup_nursing_staffing_annual_budget.job_code = safety_obs_budget_job_codes.job_code
    group by
        lookup_nursing_staffing_annual_budget.cost_center_id,
        lookup_nursing_staffing_annual_budget.cost_center_site_id,
        lookup_nursing_staffing_annual_budget.fiscal_year,
        safety_obs_budget_job_codes.safety_observer_rollup

    union all

    select
        'staffCCbdgt' as metric_abbreviation,
        cost_center_id,
        cost_center_site_id,
        fiscal_year,
        round(sum(annual_fte_budget), 2) as numerator,
        job_code,
        null as job_group_id,
        null as metric_grouper
    from
        lookup_nursing_staffing_annual_budget
    where
        budget_paycode_grouper is not null
    group by
        cost_center_id,
        cost_center_site_id,
        job_code,
        fiscal_year
),

functional_budget as (
    select
        'funcCCbdgt' as metric_abbreviation,
        null as metric_grouper,
        finance_budgets.cost_center_id,
        finance_budgets.cost_center_site_id,
        finance_budgets.fiscal_year,
        finance_budgets.numerator
            - coalesce(orientation_only_by_job_code.orientation_fte_budget, 0) as numerator,
        finance_budgets.job_code,
        finance_budgets.job_group_id
    from finance_budgets
    left join orientation_only_by_job_code
        on finance_budgets.cost_center_id = orientation_only_by_job_code.cost_center_id
        and finance_budgets.cost_center_site_id = orientation_only_by_job_code.cost_center_site_id
        and finance_budgets.job_code = orientation_only_by_job_code.job_code
        and finance_budgets.fiscal_year = orientation_only_by_job_code.fiscal_year
    where finance_budgets.metric_abbreviation = 'staffCCbdgt'
)

select
    finance_budgets.metric_abbreviation,
    nursing_pay_period.pp_end_dt_key as metric_dt_key,
    null as worker_id,
    finance_budgets.cost_center_id,
    finance_budgets.cost_center_site_id,
    finance_budgets.job_code,
    finance_budgets.job_group_id,
    finance_budgets.metric_grouper,
    finance_budgets.numerator,
    null::numeric as denominator,
    finance_budgets.numerator as row_metric_calculation
from finance_budgets as finance_budgets
inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
    on finance_budgets.fiscal_year = nursing_pay_period.fiscal_year

union all

select
    functional_budget.metric_abbreviation,
    nursing_pay_period.pp_end_dt_key as metric_dt_key,
    null as worker_id,
    functional_budget.cost_center_id,
	functional_budget.cost_center_site_id,
    functional_budget.job_code,
    functional_budget.job_group_id,
    functional_budget.metric_grouper,
    functional_budget.numerator,
    null::numeric as denominator,
    functional_budget.numerator as row_metric_calculation
from functional_budget as functional_budget
inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
    on functional_budget.fiscal_year = nursing_pay_period.fiscal_year
