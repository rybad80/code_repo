with
nicu_census as (
    select
        capacity_ip_hourly_census.census_date,
        sum(case when neo_nicu_treatment_team.treatment_team = 'NICU Blue' then 1 end) as blue_census,
        sum(case when neo_nicu_treatment_team.treatment_team = 'NICU Green' then 1 end) as green_census,
        sum(case when neo_nicu_treatment_team.treatment_team = 'NICU Orange' then 1 end) as orange_census,
        sum(case when neo_nicu_treatment_team.treatment_team = 'NICU Purple' then 1 end) as purple_census,
        sum(case when neo_nicu_treatment_team.treatment_team = 'NICU Red' then 1 end) as red_census,
        sum(case when neo_nicu_treatment_team.treatment_team = 'NICU Yellow' then 1 end) as yellow_census,
        sum(case when neo_nicu_treatment_team.treatment_team is null then 1 end) as missing_team_census,
        count(*) as total_nicu_census
    from
        {{ ref('capacity_ip_hourly_census') }} as capacity_ip_hourly_census
        left join {{ ref('neo_nicu_treatment_team') }} as neo_nicu_treatment_team
            on neo_nicu_treatment_team.visit_key = capacity_ip_hourly_census.visit_key
            and neo_nicu_treatment_team.treatment_team_start_date
                <= capacity_ip_hourly_census.census_date + interval '12 hours'
            and coalesce(neo_nicu_treatment_team.treatment_team_end_date, current_date)
                >= capacity_ip_hourly_census.census_date + interval '12 hours'
    where
        capacity_ip_hourly_census.census_date >= date '2015-01-01'
        and capacity_ip_hourly_census.department_group_name = 'NICU'
        and capacity_ip_hourly_census.census_hour = 12
    group by
        capacity_ip_hourly_census.census_date
),
nicu_days as (
    select
        census_date,
        count(distinct visit_key) as nicu_patient_days
    from
        {{ ref('capacity_ip_hourly_census') }}
    where
        census_date >= date '2015-01-01'
        and department_group_name = 'NICU'
    group by
        census_date
),
nicu_admissions as (
    select
        date(neo_nicu_episode.episode_start_date) as census_date,
        sum(case when neo_nicu_episode.nicu_episode_number = 1 then 1 end) as initial_nicu_admissions,
        count(*) as all_nicu_admissions
    from
        {{ ref('neo_nicu_episode') }} as neo_nicu_episode
    group by
        date(neo_nicu_episode.episode_start_date)
),
nicu_vent_days as (
    select
        date(recorded_date) as census_date,
        count(distinct visit_key) as nicu_vent_days
    from
        {{ ref('neo_nicu_respiratory_category') }}
    where
        date(recorded_date) >= date '2015-01-01'
        and respiratory_support_type = 'invasive'
    group by
        date(recorded_date)
)
select
    nicu_census.census_date,
    coalesce(nicu_admissions.initial_nicu_admissions, 0) as initial_nicu_admissions,
    coalesce(nicu_admissions.all_nicu_admissions, 0) as all_nicu_admissions,
    coalesce(nicu_days.nicu_patient_days, 0) as nicu_patient_days,
    coalesce(nicu_vent_days.nicu_vent_days, 0) as nicu_vent_days,
    coalesce(nicu_census.blue_census, 0) as blue_census,
    coalesce(nicu_census.green_census, 0) as green_census,
    coalesce(nicu_census.orange_census, 0) as orange_census,
    coalesce(nicu_census.purple_census, 0) as purple_census,
    coalesce(nicu_census.red_census, 0) as red_census,
    coalesce(nicu_census.yellow_census, 0) as yellow_census,
    coalesce(nicu_census.missing_team_census, 0) as missing_team_census,
    nicu_census.total_nicu_census
from
    nicu_census
    left join nicu_admissions
        on nicu_admissions.census_date = nicu_census.census_date
    left join nicu_days
        on nicu_days.census_date = nicu_census.census_date
    left join nicu_vent_days
        on nicu_vent_days.census_date = nicu_census.census_date
