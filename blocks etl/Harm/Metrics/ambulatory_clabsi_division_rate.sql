with
individual_denom as (

select
    outpatient_central_line_days_by_division.month_dt,
    outpatient_central_line_days_by_division.display_specialty,
    sum(outpatient_central_line_days_by_division.op_line_days) as op_line_days,
    sum(outpatient_central_line_days_by_division.op_line_days) / 1000 as op_line_days_per1000
from
    {{ref('outpatient_central_line_days_by_division')}} as outpatient_central_line_days_by_division
group by
    outpatient_central_line_days_by_division.month_dt,
    outpatient_central_line_days_by_division.display_specialty
),

combo_denom as (

select
    outpatient_central_line_days_by_division.month_dt,
    'IRP & NON-IRP' as display_specialty,
    sum(outpatient_central_line_days_by_division.op_line_days) as op_line_days,
    sum(outpatient_central_line_days_by_division.op_line_days) / 1000 as op_line_days_per1000
from
    {{ref('outpatient_central_line_days_by_division')}} as outpatient_central_line_days_by_division
where
    display_specialty in ('IRP', 'NON-IRP')
group by
    outpatient_central_line_days_by_division.month_dt
),

denom as (
select * from individual_denom
union all
select * from combo_denom
),

individual_nume as (

select
    date_trunc('month', ambulatory_clabsi_events.infection_date) as month_dt,
    ambulatory_clabsi_events.display_specialty,
    count(ambulatory_clabsi_events.inf_surv_id) as amb_clabsi_count,
    count(case when ambulatory_clabsi_events.mbi_lcbi_ind = 0 then ambulatory_clabsi_events.inf_surv_id
        end) as amb_clabsi_non_mbi_count
from
    {{ref('ambulatory_clabsi_events')}} as ambulatory_clabsi_events
group by
    date_trunc('month', ambulatory_clabsi_events.infection_date),
    ambulatory_clabsi_events.display_specialty
),

combo_nume as (

select
    date_trunc('month', ambulatory_clabsi_events.infection_date) as month_dt,
    'IRP & NON-IRP' as display_specialty,
    count(ambulatory_clabsi_events.inf_surv_id) as amb_clabsi_count,
    count(case when ambulatory_clabsi_events.mbi_lcbi_ind = 0 then ambulatory_clabsi_events.inf_surv_id
        end) as amb_clabsi_non_mbi_count
from
    {{ref('ambulatory_clabsi_events')}} as ambulatory_clabsi_events
where
    display_specialty in ('IRP', 'NON-IRP')
group by
    date_trunc('month', ambulatory_clabsi_events.infection_date)
),

nume as (
select * from individual_nume
union all
select * from combo_nume
),

rates as (
select
    denom.month_dt,
    denom.op_line_days,
    denom.op_line_days_per1000,
    denom.display_specialty,
    coalesce(nume.amb_clabsi_count, 0) as amb_clabsi,
    coalesce(nume.amb_clabsi_non_mbi_count, 0) as amb_clabsi_non_mbi,
    (coalesce(nume.amb_clabsi_count, 0) / denom.op_line_days_per1000) as amb_clabsi_rate_per1000,
    (coalesce(nume.amb_clabsi_non_mbi_count, 0) / denom.op_line_days_per1000) as amb_clabsi_non_mbi_rate_per1000
from
    denom
    left join nume
        on nume.month_dt = denom.month_dt
        and nume.display_specialty = denom.display_specialty
where
    denom.display_specialty in ('DIALYSIS', 'ONCOLOGY', 'IRP', 'NON-IRP', 'IRP & NON-IRP')
),

baseline_dates as (

select
    master_date.full_dt,
    division_baseline_dates.area,
    max(case when division_baseline_dates.category = 'baseline_range' then division_baseline_dates.period
        end) as period,
    max(case when division_baseline_dates.category = 'baseline_calc' then division_baseline_dates.period
        end) as calc
from
    {{ref('lookup_harm_baseline_dates')}} as division_baseline_dates
    left join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt between date(division_baseline_dates.start_date)
            and date(division_baseline_dates.end_date)
        and master_date.day_of_mm = 1 -- just need first day of month
        and master_date.full_dt < current_date
where
    division_baseline_dates.metric = 'amb_clabsi'
    and division_baseline_dates.area in ('IRP', 'NON-IRP', 'DIALYSIS', 'ONCOLOGY', 'IRP & NON-IRP')
group by
    master_date.full_dt,
    division_baseline_dates.area
),

baseline_means as (

select
    calc,
    display_specialty,
    -- Careful, don't do an average of averages
    sum(rates.amb_clabsi_non_mbi) / sum(rates.op_line_days_per1000) as non_mbi_baseline
from
    rates
    inner join baseline_dates
        on baseline_dates.full_dt = rates.month_dt
        and baseline_dates.area = rates.display_specialty
group by
    calc,
    display_specialty
)

select
    rates.month_dt,
    rates.display_specialty,
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
        as non_mbi_lower_limit
from
    rates
    inner join baseline_dates
        on baseline_dates.full_dt = rates.month_dt
        and baseline_dates.area = rates.display_specialty
    inner join baseline_means
        on baseline_means.calc = baseline_dates.period
        and baseline_means.display_specialty = baseline_dates.area
