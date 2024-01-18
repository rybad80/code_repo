with cohort as (--region
    select
        transplant_recipients.*
    from
        {{ ref('transplant_recipients') }} as transplant_recipients
    where
        transplant_recipients.organ = 'KIDNEY'
        and upper(transplant_recipients.recipient_donor) = 'RECIPIENT'
        and transplant_recipients.deceased_ind = '0'
        /*ACTIVE FOLLOW UP TRANSPLANT PATIENTS OR PATIENTS*/
        and (transplant_recipients.curr_stage = 'Transplanted'
            and transplant_recipients.phoenix_episode_status = 'Active Follow-up')
--end region
),
seen_during_flu_season as (
    select
        usnews_metadata_calendar.submission_year,
        usnews_metadata_calendar.division,
        usnews_metadata_calendar.metric_name,
        usnews_metadata_calendar.question_number,
        usnews_metadata_calendar.metric_id,
        usnews_metadata_calendar.start_date,
        stg_patient.pat_key,
        cohort.mrn,
        stg_patient.patient_name,
        stg_patient.dob,
        (date(end_date) - date(stg_patient.dob)) / 365.25 as age_at_year_end,
        usnews_metadata_calendar.age_lt,
        cohort.most_recent_transplant_date,
        max(case when vaccination_all.received_date >= usnews_metadata_calendar.start_date - interval '3 months'
        and vaccination_all.received_date <= usnews_metadata_calendar.end_date
        then vaccination_all.received_date else null end) as vaccination_date,
        max(case when
            vaccination_all.received_date is not null
            and vaccination_all.received_date >= usnews_metadata_calendar.start_date - interval '3 months'
            and vaccination_all.received_date <= usnews_metadata_calendar.end_date
        then 1 else 0 end) as flu_ind,
        max(stg_encounter.encounter_date) as last_seen_clinic_date
    from
        cohort
        inner join {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
            on lower(usnews_metadata_calendar.question_number) = 'g34.1'
        inner join {{ ref('stg_patient') }} as stg_patient
            on cohort.pat_key = stg_patient.pat_key
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on cohort.pat_key = stg_encounter.pat_key
        left join {{ ref('vaccination_all') }} as vaccination_all
            on cohort.mrn = vaccination_all.mrn
    where
        (date(usnews_metadata_calendar.end_date) - date(stg_patient.dob)) / 365.25 between 0.5 and 21
        and stg_encounter.encounter_type_id in (101, 50) -- office visit/appointment
        and stg_encounter.appointment_status_id in (2, 6) --'completed/ arrived
        and stg_encounter.department_id in (
        101012142,  --bgr nephrology
        101012185, --buc nephrology
        101012089,  --bwv nephrology
        101012023,  --kop nephrology
        89375022,   --main nephrology -- last encounter date is 7/12/2018
        101022052,  --pnj nephrology
        101022049,  --virtua nephrology
        82377022,   --vnj nephrology
        101012073   --wood nephrology -- last encounter date is 7/9/2018
        )
        and stg_encounter.encounter_date <= usnews_metadata_calendar.end_date
    group by
        usnews_metadata_calendar.submission_year,
        usnews_metadata_calendar.division,
        usnews_metadata_calendar.metric_name,
        usnews_metadata_calendar.question_number,
        usnews_metadata_calendar.metric_id,
        usnews_metadata_calendar.start_date,
        usnews_metadata_calendar.end_date,
        usnews_metadata_calendar.age_lt,
        stg_patient.pat_key,
        cohort.mrn,
        stg_patient.patient_name,
        stg_patient.dob,
        cohort.most_recent_transplant_date
)
select
    submission_year,
    division,
    metric_name,
    question_number,
    metric_id,
    start_date,
    pat_key,
    mrn,
    patient_name,
    dob,
    age_at_year_end,
    most_recent_transplant_date,
    vaccination_date,
    flu_ind,
    last_seen_clinic_date
from
    seen_during_flu_season
where
    last_seen_clinic_date >= start_date
    and age_at_year_end < age_lt
