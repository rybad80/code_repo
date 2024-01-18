{{ config(meta = {
    'critical': true
}) }}

with dates
as (
select
    f_yyyy,
    f_mm,
    month_end_date,
    last_day(month_end_date - interval '12 month') + 1 as start_rolling12_dt,
    month_end_date as end_rolling12_dt,
    end_rolling12_dt + 1 - start_rolling12_dt as days_in_rolling_12,
    last_day(month_end_date - interval '3 month') + 1 as start_rolling3_dt,
    month_end_date as end_rolling3_dt,
    month_end_date + 1 - start_rolling3_dt as days_in_rolling_3
from
    (
    select distinct
        f_yyyy,
        f_mm,
        last_day(full_dt) as month_end_date
    from
         {{ source('cdw', 'master_date') }}
    where
        f_yyyy >= 2018
        and full_dt <= current_date
) as fy_months
),
--
month_asset_total
as (
select
    sum(amount) as amount,
    period,
    year,
    fiscal_period_end_dt,
    company_reference_id,
    case
        when cost_center_reference_id = '16190' then 'PB'
        when cost_center_reference_id = '17999' then 'HB'
    end as hb_pb_ind
from
    {{ref('stg_finance_hb_pb_gl_summary')}}
where 1 = 1
    and numerator_ind = 1
    and cost_center_reference_id in ('16190', '17999')
group by
    period,
    year,
    fiscal_period_end_dt,
    company_reference_id,
    hb_pb_ind
),
--
rolling_total
as (
select
    -1 * sum(stg_finance_hb_pb_gl_summary.amount) as amount,
    dates.end_rolling3_dt as fiscal_period_end_dt,
    stg_finance_hb_pb_gl_summary.company_reference_id,
    dates.days_in_rolling_3 as number_of_rolling_days,
    3 as rolling_month_range,
    case
        when stg_finance_hb_pb_gl_summary.revenue_category_reference_id in ('RC_4000', 'RC_4001') then 'HB'
        when stg_finance_hb_pb_gl_summary.revenue_category_reference_id in ('RC_4003') then 'PB'
    end as hb_pb_ind
from
    {{ref('stg_finance_hb_pb_gl_summary')}} as stg_finance_hb_pb_gl_summary
    inner join dates on
        stg_finance_hb_pb_gl_summary.fiscal_period_end_dt between dates.start_rolling3_dt and dates.end_rolling3_dt
where 1 = 1
    and stg_finance_hb_pb_gl_summary.denominator_ind = 1
    and stg_finance_hb_pb_gl_summary.revenue_category_reference_id in ('RC_4000', 'RC_4001', 'RC_4003')
group by
	dates.end_rolling3_dt,
	company_reference_id,
	days_in_rolling_3,
	hb_pb_ind
--
union all
--
select
    -1 * sum(stg_finance_hb_pb_gl_summary.amount) as amount,
    dates.end_rolling12_dt as fiscal_period_end_dt,
    stg_finance_hb_pb_gl_summary.company_reference_id,
    dates.days_in_rolling_12 as number_of_rolling_days,
    12 as rolling_month_range,
    case
        when stg_finance_hb_pb_gl_summary.revenue_category_reference_id in ('RC_4000', 'RC_4001') then 'HB'
        when stg_finance_hb_pb_gl_summary.revenue_category_reference_id in ('RC_4003') then 'PB'
    end as hb_pb_ind
from
    {{ref('stg_finance_hb_pb_gl_summary')}} as stg_finance_hb_pb_gl_summary
    inner join dates on
    stg_finance_hb_pb_gl_summary.fiscal_period_end_dt between dates.start_rolling12_dt and dates.end_rolling12_dt
where 1 = 1
    and stg_finance_hb_pb_gl_summary.denominator_ind = 1
    and stg_finance_hb_pb_gl_summary.revenue_category_reference_id in ('RC_4000', 'RC_4001', 'RC_4003')
group by
        dates.end_rolling12_dt,
        company_reference_id,
        days_in_rolling_12,
        hb_pb_ind
)
--
select
    month_asset_total.company_reference_id,
    month_asset_total.hb_pb_ind,
    month_asset_total.fiscal_period_end_dt,
    rolling_total.rolling_month_range,
    month_asset_total.amount as monthly_asset_balance,
    rolling_total.amount as rolling_net_patient_revenue,
    rolling_total.number_of_rolling_days,
    rolling_total.amount / rolling_total.number_of_rolling_days as avg_net_revenue_per_day,
    month_asset_total.amount / avg_net_revenue_per_day as days_in_ar,
    current_timestamp as update_date
from
    month_asset_total
inner join
rolling_total on
    month_asset_total.fiscal_period_end_dt = rolling_total.fiscal_period_end_dt
    and month_asset_total.hb_pb_ind = rolling_total.hb_pb_ind
