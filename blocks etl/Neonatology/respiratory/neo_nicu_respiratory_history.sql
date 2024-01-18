/* neo_nicu_respiratory_history
A collapsed version of neo_nicu_respiratory_category, with one record
per patient per respiratory_support_category.
*/

with resp_cat_prior_next as (
    select
        visit_key,
        recorded_date,
        respiratory_support_type,
        respiratory_support_category,
        lag(respiratory_support_category) over (
            partition by visit_key
            order by recorded_date
        ) as prior_resp_support,
        lead(respiratory_support_category) over (
            partition by visit_key
            order by recorded_date
        ) as next_resp_support
    from
        {{ ref('neo_nicu_respiratory_category') }}
),

resp_cat_starts as (
    select
        visit_key,
        recorded_date,
        respiratory_support_type,
        respiratory_support_category
    from
        resp_cat_prior_next
    where
        respiratory_support_category != prior_resp_support
        or (
            respiratory_support_category is not null
            and prior_resp_support is null
        )
),

resp_cat_stops as (
    select
        visit_key,
        recorded_date,
        respiratory_support_category
    from
        resp_cat_prior_next
    where
        respiratory_support_category != next_resp_support
        or (
            respiratory_support_category is not null
            and next_resp_support is null
        )
),

resp_episodes as (
    select
        resp_cat_starts.visit_key,
        resp_cat_starts.respiratory_support_type,
        resp_cat_starts.respiratory_support_category,
        resp_cat_starts.recorded_date as resp_support_start_datetime,
        min(resp_cat_stops.recorded_date) as resp_support_stop_datetime
    from
        resp_cat_starts
        inner join resp_cat_stops
            on resp_cat_stops.visit_key = resp_cat_starts.visit_key
                and resp_cat_stops.respiratory_support_category = resp_cat_starts.respiratory_support_category
                and resp_cat_stops.recorded_date >= resp_cat_starts.recorded_date
    group by
        resp_cat_starts.visit_key,
        resp_cat_starts.respiratory_support_type,
        resp_cat_starts.respiratory_support_category,
        resp_cat_starts.recorded_date

)

select
    stg_neo_nicu_visit_demographics.visit_key,
    stg_neo_nicu_visit_demographics.patient_name,
    stg_neo_nicu_visit_demographics.mrn,
    stg_neo_nicu_visit_demographics.dob,
    stg_neo_nicu_visit_demographics.sex,
    stg_neo_nicu_visit_demographics.gestational_age_complete_weeks,
    stg_neo_nicu_visit_demographics.gestational_age_remainder_days,
    stg_neo_nicu_visit_demographics.birth_weight_grams,
    stg_neo_nicu_visit_demographics.hospital_admit_date,
    stg_neo_nicu_visit_demographics.hospital_discharge_date,
    resp_episodes.respiratory_support_type,
    resp_episodes.respiratory_support_category,
    resp_episodes.resp_support_start_datetime,
    resp_episodes.resp_support_stop_datetime,
    {{
       dbt_chop_utils.datetime_diff(
            from_date='resp_episodes.resp_support_start_datetime',
            to_date='resp_episodes.resp_support_stop_datetime',
            unit='hour'
        )
    }} as resp_support_duration_hours,
    stg_neo_nicu_visit_demographics.pat_key
from
    resp_episodes
    inner join {{ ref('stg_neo_nicu_visit_demographics') }} as stg_neo_nicu_visit_demographics
        on stg_neo_nicu_visit_demographics.visit_key = resp_episodes.visit_key
