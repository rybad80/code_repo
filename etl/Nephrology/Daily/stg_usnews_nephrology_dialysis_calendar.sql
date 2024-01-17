with stage as (
    select distinct
        usnews_metadata_calendar.submission_year,
        usnews_metadata_calendar.start_date,
        usnews_metadata_calendar.end_date,
        usnews_metadata_calendar.division,
        usnews_metadata_calendar.metric_name,
        usnews_metadata_calendar.question_number,
        usnews_metadata_calendar.age_gte,
        usnews_metadata_calendar.age_lt,
        usnews_metadata_calendar.sex,
        usnews_metadata_calendar.billing_service,
        usnews_metadata_calendar.metric_id,
        usnews_metadata_calendar.num_calculation,
        usnews_metadata_calendar.denom_calculation,
        usnews_metadata_calendar.direction,
        nephrology_encounter_dialysis.calendar_year,
        nephrology_encounter_dialysis.pat_key,
        nephrology_encounter_dialysis.patient_name,
        nephrology_encounter_dialysis.mrn,
        nephrology_encounter_dialysis.dob,
        nephrology_encounter_dialysis.age_years,
        nephrology_encounter_dialysis.visit_key,
        nephrology_encounter_dialysis.encounter_date,
        nephrology_encounter_dialysis.age_at_year_end,
        nephrology_encounter_dialysis.death_date,
        nephrology_encounter_dialysis.most_recent_weight_recorded,
        nephrology_encounter_dialysis.dialysis_type,
        nephrology_encounter_dialysis.seq,
        nephrology_encounter_dialysis.maintenance_dialysis_start_date,
        nephrology_encounter_dialysis.most_recent_dialysis_type,
        nephrology_encounter_dialysis.usnwr_flu_season_ind,
        nephrology_encounter_dialysis.most_recent_transplant_date,
        max(most_recent_weight_recorded) over (partition by submission_year, pat_key)
            as max_submission_year_weight,
        max(age_years) over (partition by submission_year, pat_key) as max_submission_year_age,
        max(age_at_year_end) over (partition by submission_year, pat_key) as max_submission_age_at_year_end
    from
        {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
    inner join
        {{ref('nephrology_encounter_dialysis')}} as nephrology_encounter_dialysis
            on  nephrology_encounter_dialysis.encounter_date between usnews_metadata_calendar.start_date
                and usnews_metadata_calendar.end_date
            and question_number in (
                'g12',
                'g13',
                'g20',
                'g20.1',
                'g21',
                'g23'
                )
    where
        maintenance_dialysis_ind = 1
)

select
    *
from
    stage
where
    (question_number in ('g20','g20.1', 'g21')
    and max_submission_age_at_year_end between age_gte and age_lt)
    or (question_number in ('g12', 'g13')
    and max_submission_year_age between age_gte and age_lt)
    or question_number = 'g23'
