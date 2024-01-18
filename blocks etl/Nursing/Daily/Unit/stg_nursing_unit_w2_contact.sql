{{ config(meta = {
    'critical': false
}) }}

/*  stg_nursing_unit_w2_contact
unit metric rows for
dailyContactCensus
PPavgContactCensus
midnightContactCensus
*/
with
unique_patient_instance_count as (
select
    metric_abbreviation,
    patient_date,
    department_id,
    numerator as patient_count
from
    {{ ref('stg_nursing_patient_count') }}
where
    metric_abbreviation = 'dailyContactCensus'
),

daily_contact_census as (
    select
        pat_per_day.metric_abbreviation,
        to_char(pat_per_day.patient_date, 'yyyymmdd') as metric_dt_key,
        get_cc.workday_cost_center_id as cost_center_id,
        pat_per_day.department_id,
        round(sum(pat_per_day.patient_count), 1) as daily_contact_census_sum
    from
        unique_patient_instance_count as pat_per_day
    left join {{ ref('department_cost_center_xref') }} as get_cc
        on pat_per_day.patient_date = get_cc.department_align_date
        and pat_per_day.department_id = get_cc.department_id
    group by
        pat_per_day.metric_abbreviation,
        pat_per_day.patient_date,
        get_cc.workday_cost_center_id,
        pat_per_day.department_id
),

pp_avg_contact_census as (
    select
        'PPavgContactCensus' as metric_abbreviation,
        nursing_pay_period.pp_end_dt_key as metric_dt_key,
        cost_center_id,
        count(distinct daily_contact_census.metric_dt_key) as census_days,
        round(sum(daily_contact_census_sum) / census_days, 1) as pp_avg_contact_census_sum
    from daily_contact_census
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on daily_contact_census.metric_dt_key
        between nursing_pay_period.pp_start_dt and nursing_pay_period.pp_end_dt
    group by
        nursing_pay_period.pp_end_dt_key,
        cost_center_id
),

midnight_census as (
    select
        'midnightCensus' as metric_abbreviation,
        to_char(capacity_ip_midnight_census.midnight_date, 'yyyymmdd') as metric_dt_key,
        get_cc.workday_cost_center_id as cost_center_id,
        capacity_ip_midnight_census.department_id,
        count(capacity_ip_midnight_census.visit_key) as midnight_census_count
    from
        {{ ref('capacity_ip_midnight_census') }} as capacity_ip_midnight_census
    left join
        {{ ref('department_cost_center_xref') }} as get_cc
        on capacity_ip_midnight_census.midnight_date = get_cc.department_align_date
        and capacity_ip_midnight_census.department_id = get_cc.department_id
    where
        metric_dt_key >= 20191215
    group by
        metric_dt_key,
        get_cc.workday_cost_center_id,
        capacity_ip_midnight_census.department_id
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    department_id,
    null as job_code,
    null as job_group_id,
    null as metric_grouper,
    daily_contact_census_sum as numerator,
    null::numeric as denominator,
    daily_contact_census_sum as row_metric_calculation
from
    daily_contact_census

union all

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as department_id,
    null as job_code,
    null as job_group_id,
    null as metric_grouper,
    pp_avg_contact_census_sum as numerator,
    null::numeric as denominator,
    pp_avg_contact_census_sum as row_metric_calculation
from
    pp_avg_contact_census

union all

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    department_id,
    null as job_code,
    null as job_group_id,
    null as metric_grouper,
    midnight_census_count as numerator,
    null::numeric as denominator,
    midnight_census_count as row_metric_calculation
from
    midnight_census
