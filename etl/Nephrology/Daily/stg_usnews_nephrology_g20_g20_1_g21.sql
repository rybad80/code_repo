select
    pat_key,
    patient_name,
    mrn,
    dob,
    visit_key,
    encounter_date,
    max_submission_age_at_year_end, -- time period spans two years
    death_date,
    most_recent_transplant_date,
    max_submission_year_weight, -- time period spans two years
    submission_year,
    start_date,
    end_date,
    division,
    metric_name,
    question_number,
    metric_id,
    pat_key as patient_primary_key,
    case when death_date is not null
        and death_date between
        start_date and end_date
        then pat_key else null end
        as death_primary_key,
    case when death_primary_key is null then patient_primary_key else null end as outcome_numerator,
    case when metric_id in ('g20a', 'g20b') then outcome_numerator
        when metric_id in ('g20a1', 'g20b1') then patient_primary_key
        when metric_id in ('g20a2', 'g20b2') then death_primary_key
        when metric_id in ('g20.1a', 'g20.1b') and max_submission_year_weight >= 10 then patient_primary_key
        when
            metric_id in ('g21a', 'g21b')
            and max_submission_year_weight >= 10
            and most_recent_transplant_date is not null
            then patient_primary_key
        else null end as num,
    case when metric_id in ('g21a', 'g21b') and max_submission_year_weight >= 10 then pat_key
        when metric_id in ('g20a', 'g20b', 'g20.1a', 'g20.1b') then pat_key
        else null end as denom
from
    {{ref('stg_usnews_nephrology_dialysis_calendar')}}
where
    question_number in ('g20', 'g20.1', 'g21')
    and (num is not null or denom is not null)
