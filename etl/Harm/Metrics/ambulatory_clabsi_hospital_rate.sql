with
denom as (

select
    outpatient_central_line_days.month_dt,
    sum(outpatient_central_line_days.op_line_days) as op_line_days,
    sum(outpatient_central_line_days.op_line_days) / 1000 as op_line_days_per1000
from
    {{ref('outpatient_central_line_days')}} as outpatient_central_line_days
group by
    outpatient_central_line_days.month_dt
),

nume as (

select
    date_trunc('month', ambulatory_clabsi_events.infection_date) as month_dt,
    count(ambulatory_clabsi_events.inf_surv_id) as amb_clabsi_count,
    count(case when ambulatory_clabsi_events.mbi_lcbi_ind = 0 then ambulatory_clabsi_events.inf_surv_id
        end) as amb_clabsi_non_mbi_count
from
    {{ref('ambulatory_clabsi_events')}} as ambulatory_clabsi_events
group by
    date_trunc('month', ambulatory_clabsi_events.infection_date)
),

rates as (
select
    denom.month_dt,
    denom.op_line_days,
    denom.op_line_days_per1000,
    coalesce(nume.amb_clabsi_count, 0) as amb_clabsi,
    coalesce(nume.amb_clabsi_non_mbi_count, 0) as amb_clabsi_non_mbi,
    (coalesce(nume.amb_clabsi_count, 0) / denom.op_line_days_per1000) as amb_clabsi_rate_per1000,
    (coalesce(nume.amb_clabsi_non_mbi_count, 0) / denom.op_line_days_per1000) as amb_clabsi_non_mbi_rate_per1000
from
    denom
    left join nume on nume.month_dt = denom.month_dt
),

baseline_dates as (

select
    master_date.full_dt,
    max(case when baseline_dates.category = 'baseline_range' then baseline_dates.period end) as period,
    max(case when baseline_dates.category = 'baseline_calc' then baseline_dates.period end) as calc
from
    {{ref('lookup_harm_baseline_dates')}} as baseline_dates
    left join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt between date(baseline_dates.start_date) and date(baseline_dates.end_date)
        and master_date.day_of_mm = 1 -- just need first day of month
        and master_date.full_dt < current_date
where
    baseline_dates.metric = 'amb_clabsi'
    and baseline_dates.area = 'hospital'
group by
    master_date.full_dt
),

baseline_means as (

select
    baseline_dates.calc,
    -- Careful, don't do an average of averages
    sum(rates.amb_clabsi_non_mbi) / sum(rates.op_line_days_per1000) as non_mbi_baseline
from
    rates
    inner join baseline_dates on baseline_dates.full_dt = rates.month_dt
group by
    baseline_dates.calc
)

select
    rates.month_dt,
    rates.op_line_days,
    rates.op_line_days_per1000,
    rates.amb_clabsi,
    rates.amb_clabsi_non_mbi,
    rates.amb_clabsi_rate_per1000,
    rates.amb_clabsi_non_mbi_rate_per1000,
    baseline_dates.period,
    baseline_dates.calc,
    baseline_means.non_mbi_baseline,
    baseline_means.non_mbi_baseline + (3 * sqrt(baseline_means.non_mbi_baseline / rates.op_line_days_per1000))
        as non_mbi_upper_limit,
    baseline_means.non_mbi_baseline - (3 * sqrt(baseline_means.non_mbi_baseline / rates.op_line_days_per1000))
        as non_mbi_lower_limit,
    baseline_means.non_mbi_baseline + (2 * sqrt(baseline_means.non_mbi_baseline / rates.op_line_days_per1000))
        as non_mbi_upper_warning_limit,
    baseline_means.non_mbi_baseline - (2 * sqrt(baseline_means.non_mbi_baseline / rates.op_line_days_per1000))
        as non_mbi_lower_warning_limit
from
    rates
    inner join baseline_dates on baseline_dates.full_dt = rates.month_dt
    inner join baseline_means on baseline_means.calc = baseline_dates.period
