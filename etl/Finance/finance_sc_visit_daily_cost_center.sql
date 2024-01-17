{{ config(meta = {
    'critical': true
}) }}

with cost_center_and_sites_actual as (
    select distinct
        cost_center_id,
        cost_center_description,
        cost_center_site_id,
        cost_center_site_name
    from
        {{ref('finance_sc_visit_actual')}}
),

cost_center_and_sites_budget as (
    select distinct
        cost_center_code as cost_center_id,
        cost_center_name as cost_center_description,
        cost_center_site_id,
        cost_center_site_name
    from
        {{ref('stg_finance_daily_cost_center_budget')}}
),

cost_center_dates_actual as (
    select
        master_date.full_dt as post_date,
        cost_center_and_sites_actual.cost_center_id,
        cost_center_and_sites_actual.cost_center_site_id
    from
        {{source('cdw','master_date')}} as master_date
        cross join cost_center_and_sites_actual
    where master_date.dt_key >= 20190701
    group by
        master_date.full_dt,
        cost_center_and_sites_actual.cost_center_id,
        cost_center_and_sites_actual.cost_center_site_id
),

cost_center_dates_budget as (
    select
        master_date.full_dt as post_date,
        cost_center_and_sites_budget.cost_center_id,
        cost_center_and_sites_budget.cost_center_site_id
    from
        {{source('cdw','master_date')}} as master_date
        cross join cost_center_and_sites_budget
    where master_date.dt_key >= 20190701
    group by
        master_date.full_dt,
        cost_center_and_sites_budget.cost_center_id,
        cost_center_and_sites_budget.cost_center_site_id
),

cost_center_dates_pt_day as (
    select
        cost_center_dates_budget.post_date,
        cost_center_dates_budget.cost_center_id,
        cost_center_and_sites_budget.cost_center_description,
        cost_center_dates_budget.cost_center_site_id,
        cost_center_and_sites_budget.cost_center_site_name,
        stg_finance_daily_cost_center_budget.company_id,
        stg_finance_daily_cost_center_budget.metric_name as specialty_care_visit_type
    from
        cost_center_dates_budget
        inner join {{ref('stg_finance_daily_cost_center_budget')}} as stg_finance_daily_cost_center_budget
            on stg_finance_daily_cost_center_budget.post_date = cost_center_dates_budget.post_date
            and stg_finance_daily_cost_center_budget.cost_center_code = cost_center_dates_budget.cost_center_id
            and stg_finance_daily_cost_center_budget.cost_center_site_id
                = cost_center_dates_budget.cost_center_site_id
            and stg_finance_daily_cost_center_budget.statistic_code in ('34', '35')
            and stg_finance_daily_cost_center_budget.cost_center_code != 50000
        inner join cost_center_and_sites_budget
            on cost_center_dates_budget.cost_center_id = cost_center_and_sites_budget.cost_center_id
            and cost_center_dates_budget.cost_center_site_id = cost_center_and_sites_budget.cost_center_site_id
    union
    select
        cost_center_dates_actual.post_date,
        cost_center_dates_actual.cost_center_id,
        cost_center_and_sites_actual.cost_center_description,
        cost_center_dates_actual.cost_center_site_id,
        cost_center_and_sites_actual.cost_center_site_name,
        finance_sc_visit_actual.company_id,
        finance_sc_visit_actual.statistic_name as specialty_care_visit_type
    from
        cost_center_dates_actual
        inner join {{ref('finance_sc_visit_actual')}} as finance_sc_visit_actual
            on finance_sc_visit_actual.post_date = cost_center_dates_actual.post_date
            and finance_sc_visit_actual.cost_center_id = cost_center_dates_actual.cost_center_id
            and finance_sc_visit_actual.cost_center_site_id
                = cost_center_dates_actual.cost_center_site_id
        inner join cost_center_and_sites_actual
            on cost_center_dates_actual.cost_center_id = cost_center_and_sites_actual.cost_center_id
            and cost_center_dates_actual.cost_center_site_id = cost_center_and_sites_actual.cost_center_site_id
    where
        finance_sc_visit_actual.revenue_statistic_ind = 1
),

actual_agg as (
    select
        finance_sc_visit_actual.post_date,
        finance_sc_visit_actual.cost_center_id,
        finance_sc_visit_actual.cost_center_description,
        finance_sc_visit_actual.cost_center_site_id,
        finance_sc_visit_actual.cost_center_site_name,
        finance_sc_visit_actual.statistic_name as specialty_care_visit_type,
        sum(finance_sc_visit_actual.specialty_care_visit_actual) as specialty_care_visit_actual
    from
        {{ref('finance_sc_visit_actual')}} as finance_sc_visit_actual
    where
        finance_sc_visit_actual.revenue_statistic_ind = 1
    group by
        finance_sc_visit_actual.post_date,
        finance_sc_visit_actual.cost_center_id,
        finance_sc_visit_actual.cost_center_description,
        finance_sc_visit_actual.cost_center_site_id,
        finance_sc_visit_actual.cost_center_site_name,
        finance_sc_visit_actual.statistic_name
)

select
    date_trunc('month', cost_center_dates_pt_day.post_date) as post_date_month,
    cost_center_dates_pt_day.post_date,
    cost_center_dates_pt_day.company_id,
    cost_center_dates_pt_day.cost_center_id,
    cost_center_dates_pt_day.cost_center_description,
    cost_center_dates_pt_day.cost_center_site_id,
    cost_center_dates_pt_day.cost_center_site_name,
    cost_center_dates_pt_day.specialty_care_visit_type,
    stg_finance_daily_cost_center_budget.metric_budget_value as specialty_care_visit_budget,
    actual_agg.specialty_care_visit_actual
from
    cost_center_dates_pt_day
    left join actual_agg
        on actual_agg.post_date = cost_center_dates_pt_day.post_date
        and actual_agg.cost_center_id = cost_center_dates_pt_day.cost_center_id
        and actual_agg.cost_center_site_id = cost_center_dates_pt_day.cost_center_site_id
        and actual_agg.specialty_care_visit_type = cost_center_dates_pt_day.specialty_care_visit_type
    left join {{ref('stg_finance_daily_cost_center_budget')}} as stg_finance_daily_cost_center_budget
        on stg_finance_daily_cost_center_budget.post_date = cost_center_dates_pt_day.post_date
        and stg_finance_daily_cost_center_budget.cost_center_code = cost_center_dates_pt_day.cost_center_id
        and stg_finance_daily_cost_center_budget.cost_center_site_id = cost_center_dates_pt_day.cost_center_site_id
        and stg_finance_daily_cost_center_budget.metric_name = cost_center_dates_pt_day.specialty_care_visit_type
        and stg_finance_daily_cost_center_budget.statistic_code in (34, 35)
