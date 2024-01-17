with stage as (
    select distinct
        usnews_metadata_calendar.submission_year,
        usnews_metadata_calendar.division,
        usnews_metadata_calendar.question_number,
        usnews_metadata_calendar.metric_id,
        usnews_metadata_calendar.start_date,
        usnews_metadata_calendar.end_date,
        stg_encounter.csn,
        stg_encounter.visit_key,
        stg_encounter.pat_key,
        stg_encounter.mrn,
        stg_encounter.patient_name,
        stg_patient.dob,
        stg_encounter.encounter_date,
        stg_encounter.encounter_type,
        stg_encounter.visit_type,
        stg_encounter.visit_type_id,
        stg_encounter.department_name,
        row_number() over(
            partition by stg_encounter.pat_key, usnews_metadata_calendar.submission_year
            order by stg_encounter.pat_key, stg_encounter.encounter_date
            ) as visit_number,
        lag(stg_encounter.encounter_date) over(
            partition by stg_encounter.pat_key, usnews_metadata_calendar.submission_year
            order by stg_encounter.pat_key, stg_encounter.encounter_date
            ) as prev_visit,
        case
            when extract(epoch from stg_encounter.encounter_date - prev_visit) / (365.25) >= 3 then 1 --noqa: prs
            when visit_number = 1 then 1
        else 0
        end as new_nephrology_patient_ind
    from
        {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        inner join {{ref('stg_encounter')}} as stg_encounter on usnews_metadata_calendar.question_number = 'g18.3'
        inner join {{ref('stg_patient')}} as stg_patient on stg_encounter.pat_key = stg_patient.pat_key
    where
        stg_encounter.encounter_date >= start_date - interval '3 years' --3 year lookback from start date
        and stg_encounter.encounter_date <= end_date
        and stg_encounter.encounter_type_id in (101, 50) -- office visit/appointment
        and stg_encounter.appointment_status_id in (2, 6) --'completed/ arrived
        and stg_encounter.department_id in (
            101012142,  --bgr nephrology
            101012185, --buc nephrology -- include per barbara maersch 1/23/22
            101012089,  --bwv nephrology
            101012023,  --kop nephrology
            89375022,   --main nephrology -- last encounter date is 7/12/2018
            101022052,  --pnj nephrology
            101022049,  --virtua nephrology
            82377022,   --vnj nephrology
            101012073   --wood nephrology -- last encounter date is 7/9/2018
        )
)
select
    *
from stage
where
    encounter_date >= start_date
    and encounter_date <= end_date
    and new_nephrology_patient_ind = 1
