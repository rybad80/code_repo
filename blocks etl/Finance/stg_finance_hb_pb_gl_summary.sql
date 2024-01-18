{{ config(meta = {
    'critical': true
}) }}

with ledger_accounts
as (
select
    stg_finance_ledger_account_levels.ledger_account_id,
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
    -- below two accounts net out with each other and dont have rev category hence excluded.
    and stg_finance_ledger_account_levels.ledger_account_id not in (51500, 59000)
)
--
select
    sum(fact_gl_summary.amount) as amount,
    fact_gl_summary.period,
    fact_gl_summary.year,
    last_day(date(case
        when fact_gl_summary.period < '07-JAN' then fact_gl_summary.year - 1
        else fact_gl_summary.year
    end || '-' || fact_gl_summary.period)) as fiscal_period_end_dt,
    fact_gl_summary.ledger_account_id,
    fact_gl_summary.company_reference_id,
    fact_gl_summary.cost_center_reference_id,
    fact_gl_summary.revenue_category_reference_id,
    ledger_accounts.numerator_ind,
    ledger_accounts.denominator_ind
from
    {{ref('fact_gl_summary')}} as fact_gl_summary
    inner join ledger_accounts on fact_gl_summary.ledger_account_id  = ledger_accounts.ledger_account_id
where
    company_reference_id = '100'
group by
    fact_gl_summary.period,
    fact_gl_summary.year,
    fact_gl_summary.ledger_account_id,
    fact_gl_summary.company_reference_id,
    fact_gl_summary.cost_center_reference_id,
    fiscal_period_end_dt,
    fact_gl_summary.revenue_category_reference_id,
    ledger_accounts.numerator_ind,
    ledger_accounts.denominator_ind
