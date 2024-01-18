{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_budget_period_monthly
Apply the month staffing FTE budgets to individual pay period rows and
incorporate the Fixed Vacancy job code rows (negative FTEs)
*/
with align_budget_month_to_pp as (
    select
        pp_end_dt_key,
        pp_end_dt,
        master_date.f_yyyy,
        master_date.f_mm
    from
        {{ ref('nursing_pay_period') }} as nursing_pay_period
        inner join {{ source('cdw', 'master_date') }} as master_date
            on nursing_pay_period.pp_end_dt = master_date.full_dt
            and nursing_pay_period.nccs_platform_window_ind = 1
            and (nursing_pay_period.prior_fiscal_year_ind = 1
            or nursing_pay_period.current_fiscal_year_ind = 1)
)

select
    /* Fixed Vacancy always fixed */
    'JgBdgtPrdctvFixed' as metric_abbreviation,
    stg_nursing_budget_period_workforce.metric_dt_key,
    null as worker_id,
    stg_nursing_budget_period_workforce.cost_center_id,
    null as cost_center_site_id,
    stg_nursing_budget_period_workforce.job_code,
    '@FixedVacancy' as job_group_id,
    null as metric_grouper,
    stg_nursing_budget_period_workforce.numerator,
    null::numeric as denominator,
    stg_nursing_budget_period_workforce.numerator as row_metric_calculation
from
     {{ ref('stg_nursing_budget_period_workforce') }} as stg_nursing_budget_period_workforce
	inner join align_budget_month_to_pp
            on stg_nursing_budget_period_workforce.metric_dt_key = align_budget_month_to_pp.pp_end_dt_key
            and stg_nursing_budget_period_workforce.job_code = 'FVJC '
            and stg_nursing_budget_period_workforce.metric_abbreviation  = 'staffCCbdgt'

union all

select
    finance_monthly_budget.metric_abbreviation,
    align_budget_month_to_pp.pp_end_dt_key as metric_dt_key,
    null as worker_id,
    finance_monthly_budget.cost_center_id,
    finance_monthly_budget.cost_center_site_id,
    finance_monthly_budget.job_code,
    finance_monthly_budget.job_group_id,
    finance_monthly_budget.metric_grouper,
    finance_monthly_budget.numerator,
    null::numeric as denominator,
    finance_monthly_budget.numerator as row_metric_calculation
from
    {{ ref('stg_nursing_finance_staffing_monthly') }} as finance_monthly_budget
    inner join align_budget_month_to_pp
        on finance_monthly_budget.fiscal_year = align_budget_month_to_pp.f_yyyy
        and  finance_monthly_budget.fiscal_month_num = align_budget_month_to_pp.f_mm
