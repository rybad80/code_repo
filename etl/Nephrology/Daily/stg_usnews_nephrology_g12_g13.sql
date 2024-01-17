-- flu
with flu as (
    select distinct
        stg_usnews_nephrology_dialysis_calendar.submission_year,
        stg_usnews_nephrology_dialysis_calendar.division,
        stg_usnews_nephrology_dialysis_calendar.question_number,
        stg_usnews_nephrology_dialysis_calendar.pat_key,
        stg_usnews_nephrology_dialysis_calendar.mrn,
        stg_usnews_nephrology_dialysis_calendar.patient_name,
        stg_usnews_nephrology_dialysis_calendar.dob,
        stg_usnews_nephrology_dialysis_calendar.age_gte,
        stg_usnews_nephrology_dialysis_calendar.age_lt,
        (date(end_date) - date(stg_usnews_nephrology_dialysis_calendar.dob)) / 365.25 as age_at_year_end,
        stg_usnews_nephrology_dialysis_calendar.most_recent_dialysis_type,
        stg_usnews_nephro_vaccinations.immunization_date as flu_vaccine_date,
        null as pneumovax_vaccine_date,
        null as prevnar_13_vaccine_date_1,
        null as prevnar_13_vaccine_date_2,
        null as prevnar_13_vaccine_date_3,
        null as prevnar_13_vaccine_date_4,
        case when stg_usnews_nephrology_dialysis_calendar.most_recent_dialysis_type = 'HD'
            then 'g12a2' else 'g12b2' end as metric_id
    from
        {{ref('stg_usnews_nephrology_dialysis_calendar')}} as stg_usnews_nephrology_dialysis_calendar
        left join {{ref('stg_usnews_nephro_vaccinations')}} as stg_usnews_nephro_vaccinations
            on stg_usnews_nephrology_dialysis_calendar.pat_key = stg_usnews_nephro_vaccinations.pat_key
                and year(immunization_date) = year(stg_usnews_nephrology_dialysis_calendar.encounter_date)
                /* Below requires the flu vaccine be administered between Aug 1 and Dec 31 */
                and immunization_date between date(year(start_date) || '-08-01') and end_date
                and grouper_records_numeric_id not in (28, 89)
    where
        stg_usnews_nephrology_dialysis_calendar.usnwr_flu_season_ind = 1
        and question_number = 'g12'
),
/*
 * Pulling prevnars separately to add all four prevnar dates, which is required for "fully vaccinated" logic
 */
prevnar as (
    select
        stg_usnews_nephro_vaccinations.pat_key,
        stg_usnews_nephro_vaccinations.immunization_date,
        row_number() over(partition by stg_usnews_nephro_vaccinations.pat_key
            order by stg_usnews_nephro_vaccinations.immunization_date)
                as prevnar_order
    from {{ref('stg_usnews_nephro_vaccinations')}} as stg_usnews_nephro_vaccinations
    where stg_usnews_nephro_vaccinations.grouper_records_numeric_id = 89
),
pneumococcal as (
    select
        stg_usnews_nephrology_dialysis_calendar.submission_year,
        stg_usnews_nephrology_dialysis_calendar.division,
        stg_usnews_nephrology_dialysis_calendar.question_number,
        stg_usnews_nephrology_dialysis_calendar.pat_key,
        stg_usnews_nephrology_dialysis_calendar.mrn,
        stg_usnews_nephrology_dialysis_calendar.patient_name,
        stg_usnews_nephrology_dialysis_calendar.dob,
        stg_usnews_nephrology_dialysis_calendar.age_gte,
        stg_usnews_nephrology_dialysis_calendar.age_lt,
        (date(end_date) - date(stg_usnews_nephrology_dialysis_calendar.dob)) / 365.25 as age_at_year_end,
        stg_usnews_nephrology_dialysis_calendar.most_recent_dialysis_type,
        null as flu_vaccine_date,
        max(pneumovax.immunization_date) as pneumovax_vaccine_date,
        first_prevnar.immunization_date as prevnar_13_vaccine_date_1,
        second_prevnar.immunization_date as prevnar_13_vaccine_date_2,
        third_prevnar.immunization_date as prevnar_13_vaccine_date_3,
        fourth_prevnar.immunization_date as prevnar_13_vaccine_date_4,
        case when stg_usnews_nephrology_dialysis_calendar.most_recent_dialysis_type = 'HD'
            then 'g13a2' else 'g13b2' end as metric_id
    from {{ref('stg_usnews_nephrology_dialysis_calendar')}} as stg_usnews_nephrology_dialysis_calendar
    left join {{ref('stg_usnews_nephro_vaccinations')}} as pneumovax
        on stg_usnews_nephrology_dialysis_calendar.pat_key = pneumovax.pat_key
            and immunization_date <= end_date
            and grouper_records_numeric_id = 28
    left join prevnar as first_prevnar
        on stg_usnews_nephrology_dialysis_calendar.pat_key = first_prevnar.pat_key
        and first_prevnar.prevnar_order = 1
        and first_prevnar.immunization_date <= end_date
    left join prevnar as second_prevnar
        on stg_usnews_nephrology_dialysis_calendar.pat_key = second_prevnar.pat_key
        and second_prevnar.prevnar_order = 2
        and second_prevnar.immunization_date <= end_date
    left join prevnar as third_prevnar
        on stg_usnews_nephrology_dialysis_calendar.pat_key = third_prevnar.pat_key
        and third_prevnar.prevnar_order = 3
        and third_prevnar.immunization_date <= end_date
    left join prevnar as fourth_prevnar
        on stg_usnews_nephrology_dialysis_calendar.pat_key = fourth_prevnar.pat_key
        and fourth_prevnar.prevnar_order = 4
        and fourth_prevnar.immunization_date <= end_date
    where question_number = 'g13'
    group by
        stg_usnews_nephrology_dialysis_calendar.submission_year,
        stg_usnews_nephrology_dialysis_calendar.division,
        stg_usnews_nephrology_dialysis_calendar.question_number,
        stg_usnews_nephrology_dialysis_calendar.pat_key,
        stg_usnews_nephrology_dialysis_calendar.mrn,
        stg_usnews_nephrology_dialysis_calendar.patient_name,
        stg_usnews_nephrology_dialysis_calendar.dob,
        stg_usnews_nephrology_dialysis_calendar.age_gte,
        stg_usnews_nephrology_dialysis_calendar.age_lt,
        stg_usnews_nephrology_dialysis_calendar.end_date,
        stg_usnews_nephrology_dialysis_calendar.most_recent_dialysis_type,
        flu_vaccine_date,
        prevnar_13_vaccine_date_1,
        prevnar_13_vaccine_date_2,
        prevnar_13_vaccine_date_3,
        prevnar_13_vaccine_date_4,
        metric_id
)
select
    *,
    case when flu_vaccine_date is not null then 1 else 0 end as vaccinated_ind
from flu
where
    age_at_year_end >= age_gte
    and age_at_year_end < age_lt

union all

select distinct
    *,
    /* Patient is fully vaccinated if 1 pneumovax or 4 prevnars. Else if over 6y/o, one prevnar is sufficient */
    case when pneumovax_vaccine_date is not null
        then 1
        when prevnar_13_vaccine_date_4 is not null
        then 1
        when (age_at_year_end >= 6 and prevnar_13_vaccine_date_1 is not null)
        then 1
        else 0 end as vaccinated_ind
from pneumococcal
where
    age_at_year_end >= age_gte
    and age_at_year_end < age_lt
