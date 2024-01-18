with journals as (
    select
        journal_entry_line.accounting_date,
        journal_entry_line.journal_status as journal_entry_status_id,
        journal_entry_line.journal_source_reference_id as journal_source_id,
        journal_entry_line.transaction_credit_amount as credit_amount,
        journal_entry_line.transaction_debit_amount as debit_amount,
        journal_entry_line.ledger_credit_amount,
        journal_entry_line.ledger_debit_amount,
        journal_entry_line.line_company_reference_id as line_company_id,
        journal_entry_line.cost_center_reference_id,
        journal_entry_line.cost_center_site_reference_id as cost_center_site_id,
        journal_entry_line.ledger_account_id,
        journal_entry_line.revenue_category_reference_id as revenue_category_id,
        journal_entry_line.spend_category_reference_id as spend_category_id
    from
        {{ref('fact_journal_lines_posted')}} as journal_entry_line
    where journal_entry_line.ledger_type = 'Actuals'
    and journal_entry_line.journal_status = 'Posted'
),

last_days as (
    select
        dt_key,
        f_yyyy,
        f_mm,
        date_trunc('month', full_dt) as month_full_dt
    from
        {{source('cdw', 'master_date')}}
    where
        last_day_month_ind = 1
),

companies as (
    select *
    from
        {{source('cdw', 'company')}}
    where
        lower(create_by) in ('admin', 'workday')
),

cost_centers as (
    select *
    from
        {{source('cdw', 'cost_center')}}
    where
        lower(create_by) in ('admin', 'workday')
),

summary_data as (
    select
        last_days.dt_key as fiscal_dt_key,
        last_days.f_yyyy,
        companies.comp_key,
        workday_ledger_account.ledger_acct_key,
        cost_centers.cost_cntr_key,
        workday_cost_center_site.cost_cntr_site_key,
        workday_revenue_category.revenue_cat_key,
        workday_spend_category.spend_cat_key,
        workday_ledger_account.ledger_acct_type,
        journals.journal_source_id as journal_source,
        sum(credit_amount) as credit_amount,
        sum(debit_amount) as debit_amount,
        sum(debit_amount) - sum(credit_amount) as debit_minus_credit_amount
    from
        journals
    inner join
        {{source('workday', 'workday_ledger_account')}} as workday_ledger_account
            on journals.ledger_account_id = workday_ledger_account.ledger_acct_id
    inner join
        last_days
            on date_trunc('month', journals.accounting_date) = last_days.month_full_dt
    left join
        companies
            on coalesce(journals.line_company_id, '0') = companies.comp_id
    left join
        cost_centers
            on coalesce(journals.cost_center_reference_id, '0') = cast(cost_centers.cost_cntr_id as varchar(50))
    left join
        {{source('workday', 'workday_cost_center_site')}} as workday_cost_center_site
            on coalesce(journals.cost_center_site_id, '0') = workday_cost_center_site.cost_cntr_site_id
    left join
        {{source('workday', 'workday_spend_category')}} as workday_spend_category
            on coalesce(journals.spend_category_id, '0') = workday_spend_category.spend_cat_id
    left join
        {{source('workday', 'workday_revenue_category')}} as workday_revenue_category
            on coalesce(journals.revenue_category_id, '0') = workday_revenue_category.revenue_cat_id
    group by
        last_days.dt_key,
        last_days.f_yyyy,
        companies.comp_key,
        workday_ledger_account.ledger_acct_key,
        cost_centers.cost_cntr_key,
        workday_cost_center_site.cost_cntr_site_key,
        workday_revenue_category.revenue_cat_key,
        workday_spend_category.spend_cat_key,
        workday_ledger_account.ledger_acct_type,
        journals.journal_source_id
),

gl_summary as (
    select
        fiscal_dt_key,
        f_yyyy,
        comp_key,
        ledger_acct_key,
        cost_cntr_key,
        cost_cntr_site_key,
        revenue_cat_key,
        spend_cat_key,
        ledger_acct_type,
        journal_source,
        credit_amount,
        debit_amount,
        debit_minus_credit_amount,
        sum(credit_amount) over (
            partition by f_yyyy, comp_key, ledger_acct_key, cost_cntr_key,
                cost_cntr_site_key, revenue_cat_key,
                spend_cat_key, ledger_acct_type, journal_source
            order by fiscal_dt_key
        ) as credit_amount_fytd,
        sum(debit_amount) over (
            partition by f_yyyy, comp_key, ledger_acct_key, cost_cntr_key,
                cost_cntr_site_key, revenue_cat_key,
                spend_cat_key, ledger_acct_type, journal_source
            order by fiscal_dt_key
        ) as debit_amount_fytd,
        sum(debit_minus_credit_amount) over (
            partition by f_yyyy, comp_key, ledger_acct_key, cost_cntr_key,
                cost_cntr_site_key, revenue_cat_key,
                spend_cat_key, ledger_acct_type, journal_source
            order by fiscal_dt_key
        ) as debit_minus_credit_amount_fytd
    from
        summary_data
),

gl_months as (
    select distinct
         fiscal_dt_key
    from
        gl_summary
),

dates as (
    select
        fiscal_dt_key as metric_dt_key,
        last_day(master_date.full_dt - interval '12 month') + 1  as start_rolling12_dt,
        master_date.full_dt as end_rolling12_dt,
        end_rolling12_dt + 1 - start_rolling12_dt as days_in_rolling_12,
        last_day(master_date.full_dt - interval '3 month') + 1  as start_rolling3_dt,
        master_date.full_dt as end_rolling3_dt,
        end_rolling3_dt + 1 - start_rolling3_dt as days_in_rolling_3,
        cast(to_char(start_rolling12_dt, 'yyyymmdd') as bigint) as  start_rolling12_dt_key,
        cast(to_char(end_rolling12_dt, 'yyyymmdd') as bigint) as  end_rolling12_dt_key,
        cast(to_char(full_dt - f_day + 1, 'yyyymmdd') as bigint) as fiscal_start_dt_key,
        cast(to_char(start_rolling3_dt, 'yyyymmdd') as bigint) as  start_rolling3_dt_key,
        cast(to_char(end_rolling3_dt, 'yyyymmdd') as bigint) as  end_rolling3_dt_key
    from
        {{source('cdw', 'master_date')}} as master_date
        inner join gl_months
            on master_date.dt_key = gl_months.fiscal_dt_key
    where
        gl_months.fiscal_dt_key < cast(to_char(current_date, 'yyyymmdd') as bigint)
),

metric_accounts as (
    select
        stg_finance_ledger_account_levels.ledger_account_key,
        lookup_finance_metric_ledger_account.numerator_ind,
        lookup_finance_metric_ledger_account.denominator_ind
    from
        {{ref('stg_finance_ledger_account_levels')}} as stg_finance_ledger_account_levels
        inner join
            {{ref('lookup_finance_metric_ledger_account')}} as lookup_finance_metric_ledger_account
                on
                    lookup_finance_metric_ledger_account.ledger_account_summary_id
                    = stg_finance_ledger_account_levels.ledger_account_summary_id
                    and lower(lookup_finance_metric_ledger_account.metric_name) = 'days_in_ar'
),

numerator as (
    select
        gl_summary.comp_key,
        dates.metric_dt_key,
        sum(gl_summary.debit_minus_credit_amount) as month_asset_total,
        sum(gl_summary.credit_amount) as month_asset_credit_total,
        sum(gl_summary.debit_amount) as month_asset_debit_total,
        sum(gl_summary.debit_minus_credit_amount_fytd) as fiscal_ending_balance
    from
        dates
        inner join
            gl_summary
                on gl_summary.fiscal_dt_key between dates.fiscal_start_dt_key and dates.metric_dt_key
        inner join
            metric_accounts
                on metric_accounts.ledger_account_key = gl_summary.ledger_acct_key
    where
        metric_accounts.numerator_ind = 1
    group by
        gl_summary.comp_key,
        dates.metric_dt_key
),
denominator as (
    select
        gl_summary.comp_key,
        gl_summary.fiscal_dt_key as rolling_dt_key,
        -sum(gl_summary.debit_minus_credit_amount)
            as month_income_total
    from
        gl_summary
        inner join
            metric_accounts
                on metric_accounts.ledger_account_key = gl_summary.ledger_acct_key
    where
        metric_accounts.denominator_ind = 1
    group by
        gl_summary.comp_key,
        gl_summary.fiscal_dt_key
),
metric_rows_12 as (
    select
        numerator.metric_dt_key,
        numerator.comp_key,
        max(numerator.month_asset_total) as month_asset_total,
        max(cast(dates.days_in_rolling_12 as double)) as days_for_average,
        sum(denominator.month_income_total) as rolling_income_total
    from
        numerator
        inner join
            dates
                on dates.metric_dt_key = numerator.metric_dt_key
        inner join
            denominator
                on denominator.rolling_dt_key between dates.start_rolling12_dt_key and dates.end_rolling12_dt_key
                    and numerator.comp_key = denominator.comp_key
    group by
        numerator.metric_dt_key,
        numerator.comp_key
),

metric_rows_3 as (
    select
        numerator.metric_dt_key,
        numerator.comp_key,
        max(numerator.month_asset_total) as month_asset_total,
        max(cast(dates.days_in_rolling_3 as double)) as days_for_average,
        sum(denominator.month_income_total) as rolling_income_total
    from
        numerator
        inner join
            dates
                on dates.metric_dt_key = numerator.metric_dt_key
        inner join
            denominator
                on denominator.rolling_dt_key between dates.start_rolling3_dt_key and dates.end_rolling3_dt_key
                    and numerator.comp_key = denominator.comp_key
    group by
        numerator.metric_dt_key,
        numerator.comp_key
)

select
    company.comp_nm as company_name,
    company.comp_id as company_id,
    metric_rows_12.metric_dt_key as metric_date_key,
    12 as rolling_month_range,
    metric_rows_12.month_asset_total as monthly_asset_balance,
    metric_rows_12.rolling_income_total
        as rolling_net_patient_revenue,
    metric_rows_12.days_for_average,
    metric_rows_12.rolling_income_total / metric_rows_12.days_for_average
        as avg_net_revenue_per_day,
    metric_rows_12.month_asset_total / avg_net_revenue_per_day
        as days_in_ar
from
    metric_rows_12
    inner join
        {{source('cdw', 'company')}} as company
            on metric_rows_12.comp_key = company.comp_key
where
    metric_rows_12.metric_dt_key >= 20180701

union all

select
    company.comp_nm as company_name,
    company.comp_id as company_id,
    metric_rows_3.metric_dt_key as metric_date_key,
    3 as rolling_month_range,
    metric_rows_3.month_asset_total as monthly_asset_balance,
    metric_rows_3.rolling_income_total as rolling_net_patient_revenue,
    metric_rows_3.days_for_average,
    metric_rows_3.rolling_income_total / metric_rows_3.days_for_average
        as avg_net_revenue_per_day,
    metric_rows_3.month_asset_total / avg_net_revenue_per_day
        as days_in_ar
from
    metric_rows_3
    inner join
        {{source('cdw', 'company')}} as company
            on metric_rows_3.comp_key = company.comp_key
where
    metric_rows_3.metric_dt_key >= 20180701
