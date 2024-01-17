with
neo_itcu_stays as (
    select
        visit_key,
        enter_date,
        exit_date_or_current_date
    from
        {{ ref('adt_department_group') }}
    where
        lower(department_group_name) = 'itcu'
        and lower(initial_service) = 'neonatology'
),
itcu_calendar as (
    /* need all calendar days since itcu has opened for census / patient day calcs */
    select
        master_date.full_dt as census_date
    from
        neo_itcu_stays
        inner join {{ source('cdw', 'master_date') }} as master_date
            on master_date.full_dt between date(neo_itcu_stays.enter_date)
                and date(neo_itcu_stays.exit_date_or_current_date)
    where
        master_date.full_dt < current_date
    group by
        master_date.full_dt
),
itcu_admissions as (
    select
        date(enter_date) as census_date,
        count(*) as itcu_admissions
    from
        neo_itcu_stays
    group by
        date(enter_date)
),
itcu_census as (
    select
        itcu_calendar.census_date,
        sum(
            case
                when neo_itcu_stays.enter_date <= itcu_calendar.census_date + interval '12 hours'
                    and coalesce(neo_itcu_stays.exit_date_or_current_date, current_date)
                        >= itcu_calendar.census_date + interval '12 hours'
                then 1
            end
        ) as itcu_census,
        count(distinct neo_itcu_stays.visit_key) as itcu_patient_days
    from
        itcu_calendar
        inner join neo_itcu_stays
            on neo_itcu_stays.enter_date <= itcu_calendar.census_date
            and neo_itcu_stays.exit_date_or_current_date >= itcu_calendar.census_date
    group by
        itcu_calendar.census_date
)
select
    itcu_calendar.census_date,
    coalesce(itcu_admissions.itcu_admissions, 0) as itcu_neo_admissions,
    coalesce(itcu_census.itcu_patient_days, 0) as itcu_neo_patient_days,
    coalesce(itcu_census.itcu_census, 0) as itcu_neo_census
from
    itcu_calendar
    left join itcu_census
        on itcu_census.census_date = itcu_calendar.census_date
    left join itcu_admissions
        on itcu_admissions.census_date = itcu_calendar.census_date
