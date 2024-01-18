{{ config(meta = {
    'critical': true
}) }}

with payroll_accounting_lines as (
    select
        *
    from
        {{ ref('fact_payroll_accounting_lines') }}
    where
        1 = 1
        and journal_source_reference_id != 'Payroll_Forward_Accrual'
        and earnings_in_effort_report = 1
        and earnings_in_effort_report = 1
        and ( ledger_type = 'Actuals'
           or (ledger_type = 'Obligation'
               and budget_date > '06-30-2022'
               )
            )
),
--
worker as (
    select
        *
    from
        (
        select
            rank() over (partition by worker_id
        order by
            reporting_chain desc) as rnk,
            *
        from
            {{ ref('worker') }} as worker            
        ) wrk
    where
        rnk = 1
),
--
emp_pay_period as (
    select
        lines.employee_id,
        lines.payroll_pay_period_wid,
        lines.payroll_pay_period_reference_id,
        lines.ledger_type,
        lines.pay_group_reference_id,
        min(lines.pay_period_start_date) as pay_period_start_date,
        max(lines.pay_period_end_date) as pay_period_end_date
    from
        payroll_accounting_lines as lines
    where 1 = 1
        and lines.payroll_pay_period_wid is not null
        -- dbt utility for group by
        {{ dbt_utils.group_by(n=5) }}
),
--
-- Add missing pay periods to fact_payroll_accouting_lines
-- for the payroll adjustments using the budget date:
pal_with_derived_pay_period as (
    select
        lines.*,
        coalesce(
            emp_pay_period.pay_period_start_date,
            lines.budget_date
        ) as d_pay_period_start_date,
        coalesce(
            emp_pay_period.pay_period_end_date,
            lines.budget_date
        ) as d_pay_period_end_date,
        coalesce(
            emp_pay_period.payroll_pay_period_reference_id,
            lines.budget_date::varchar(50)
        ) as d_payroll_pay_period_reference_id,
        coalesce(
            emp_pay_period.payroll_pay_period_wid,
            lines.budget_date::varchar(50)
        ) as d_payroll_pay_period_wid,
        lines.pay_period_start_date,
        emp_pay_period.pay_period_start_date
    from
        payroll_accounting_lines as lines
    left join emp_pay_period on
        lines.employee_id = emp_pay_period.employee_id
        and lines.ledger_type = emp_pay_period.ledger_type
        and lines.pay_group_reference_id = emp_pay_period.pay_group_reference_id
        and lines.budget_date
            between emp_pay_period.pay_period_start_date and emp_pay_period.pay_period_end_date
),
--
pay_by_pay_period as (
    select
        employee_id,
        d_payroll_pay_period_wid,
        ledger_type,
        pay_group_reference_id,
        count(*) as payroll_result_lines_count,
        sum(ledger_debit_amount - ledger_credit_amount) as pay_period_total_amount,
        sum(gl_hours) as pay_period_total_gl_hours,
        sum(allocated_hours) as pay_period_total_allocated_hours
    from
        pal_with_derived_pay_period
        -- dbt utility for group by
        {{ dbt_utils.group_by(n=4) }}
),
--
pal_aggregated as (
    select
        lines.employee_id,
        upper(worker.legal_reporting_name) as employee_name,
        worker.cost_center_id || '-' || worker.cost_center_name as home_cost_center,
        worker.manager_name,
        lines.pay_group_reference_id as pay_group,
        case
            when lines.project_reference_id is not null then 'project'
            when lines.grant_reference_id is not null then 'grant'
            when lines.grant_costshare_reference_id is not null then 'grant_cost_sharing'
            else 'cost_center'
        end as primary_allocation_worktag_type,
        case
            when lines.project_reference_id is not null then lines.project_reference_id
            when lines.grant_reference_id is not null then lines.grant_reference_id
            when lines.grant_costshare_reference_id is not null
                then lines.grant_costshare_reference_id
            else lines.cost_center_reference_id
        end as primary_allocation_worktag_reference_id,
        case
            when lines.project_reference_id is not null
                then dim_project.project_id || '-' || dim_project.project_name
            when lines.grant_reference_id is not null
                then dim_grant.grant_id || '-' || dim_grant.grant_name
            when lines.grant_costshare_reference_id is not null
                then dim_grant_costshare.grant_costshare_ref_id || '-' ||
                    dim_grant_costshare.grant_costshare_code
            else dim_cost_center.cost_center_code || '-' || dim_cost_center.cost_center_name
        end as primary_allocation_worktag,
        dim_company.company_key,
        dim_company.company_id || '-' || dim_company.company_name as company,
        gl_company_reference_id,
        dim_cost_center.cost_center_key,
        dim_cost_center.cost_center_code || '-' || dim_cost_center.cost_center_name as cost_center,
        dim_grant.grant_key,
        dim_grant.grant_id || '-' || dim_grant.grant_name as grant,
        dim_grant_costshare.grant_costshare_key,
        dim_grant_costshare.grant_costshare_ref_id || '-' ||
            dim_grant_costshare.grant_costshare_code as grant_costshare,
        dim_project.project_key,
        dim_project.project_id || '-' || dim_project.project_name as project,
        dim_project.inactive_ind as project_inactive,
        dim_grant.inactive_ind as grant_inactive,
        dim_cost_center.inactive_ind as cost_center_inactive,
        lines.d_pay_period_start_date as pay_period_start_date,
        lines.d_pay_period_end_date as pay_period_end_date,
        lines.d_payroll_pay_period_reference_id
            as payroll_pay_period_reference_id,
        lines.pay_group_reference_id,
        lines.ledger_type,
        round(
            case
                when sum(lines.ledger_debit_amount - lines.ledger_credit_amount ) = 0
                    or max(trunc(pay_by_pay_period.pay_period_total_amount)) = 0
                    then 0
                else sum(lines.ledger_debit_amount - lines.ledger_credit_amount) /
                    max(pay_by_pay_period.pay_period_total_amount)
            end,
            4
        ) as allocation_percent,
        allocation_percent * 100 as allocation_percent_display,
        min(lines.budget_date) as min_budget_date,
        max(lines.budget_date) as max_budget_date
    from
        pal_with_derived_pay_period as lines
    inner join worker as worker
        on lines.employee_id = worker.worker_id
    inner join {{ ref('dim_company') }} as dim_company
        on lines.company_key = dim_company.company_key
    inner join {{ ref('dim_cost_center') }} as dim_cost_center
        on lines.cost_center_key = dim_cost_center.cost_center_key
    inner join {{ ref('dim_grant') }} as dim_grant
        on lines.grant_key = dim_grant.grant_key
    inner join {{ ref('dim_grant_costshare') }} as dim_grant_costshare
        on lines.grant_costshare_key = dim_grant_costshare.grant_costshare_key
    inner join {{ ref('dim_project') }} as dim_project
        on lines.project_key = dim_project.project_key
    left join pay_by_pay_period
        on lines.d_payroll_pay_period_wid = pay_by_pay_period.d_payroll_pay_period_wid
        and lines.employee_id = pay_by_pay_period.employee_id
        and lines.ledger_type = pay_by_pay_period.ledger_type
        and lines.pay_group_reference_id  =
            pay_by_pay_period.pay_group_reference_id
        -- dbt utility for group by
        {{ dbt_utils.group_by(n=27) }}
    having
        allocation_percent != 0
)
select
    {{
        dbt_utils.surrogate_key([
            'employee_id',
            'home_cost_center',
            'manager_name',
            'primary_allocation_worktag_reference_id',
            'company_key',
            'gl_company_reference_id',
            'cost_center_key',
            'grant_key',
            'grant_costshare_key',
            'project_key',
            'payroll_pay_period_reference_id',
            'pay_group_reference_id',
            'ledger_type'
        ])
    }} as payroll_allocation_key,
    employee_id,
    employee_name,
    home_cost_center,
    manager_name,
    pay_group,
    primary_allocation_worktag_type,
    primary_allocation_worktag_reference_id,
    primary_allocation_worktag,
    company_key,
    company,
    gl_company_reference_id,
    cost_center_key,
    cost_center,
    grant_key,
    grant,
    grant_costshare_key,
    grant_costshare,
    project_key,
    project,
    project_inactive,
    grant_inactive,
    cost_center_inactive,
    allocation_percent,
    allocation_percent_display,
    min_budget_date,
    max_budget_date,
    pay_period_start_date,
    pay_period_end_date,
    payroll_pay_period_reference_id,
    pay_group_reference_id,
    ledger_type
from
    pal_aggregated

