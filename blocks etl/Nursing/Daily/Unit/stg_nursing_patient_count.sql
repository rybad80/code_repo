{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_patient_count
gather unit patient counts for:
1) IP census units' distinct visit keys per day
to support contact census
*/
with
get_min_pp_start_dt as (
    select
        min(pp_start_dt) as min_pp_start_dt
    from
        {{ ref('nursing_pay_period') }}
    where
        current_fiscal_year_ind = 1
        or prior_fiscal_year_ind = 1
),

unique_patient_instance as (
    select
        capacity_ip_hourly_census.census_date,
        capacity_ip_hourly_census.department_id,
        capacity_ip_hourly_census.visit_key
    from
        {{ ref('capacity_ip_hourly_census') }} as capacity_ip_hourly_census
        inner join get_min_pp_start_dt
            on capacity_ip_hourly_census.census_date
            >= get_min_pp_start_dt.min_pp_start_dt
    group by
        capacity_ip_hourly_census.census_date,
        capacity_ip_hourly_census.department_id,
        capacity_ip_hourly_census.visit_key
)

select
    'dailyContactCensus' as metric_abbreviation,
    census_date as patient_date,
    department_id,
    count(*) as numerator
from
    unique_patient_instance
group by
    census_date,
    department_id
