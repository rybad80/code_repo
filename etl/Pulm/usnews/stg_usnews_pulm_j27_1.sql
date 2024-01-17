with j27_cohort as ( -- patients who had at least 1 encounters with Pulm for premature lung disease diagnosis
select
    usnews_metadata_calendar.submission_year,
    enc.mrn,
    enc.patient_name,
    enc.encounter_date,
    enc.dob,
    enc.age_months,
    dx_enc.icd10_code,
    enc.pat_key,
    enc.visit_key
from
    {{ref('stg_encounter')}} as enc
    inner join {{ref('diagnosis_encounter_all')}} as dx_enc
        on enc.visit_key = dx_enc.visit_key
        and dx_enc.visit_diagnosis_ind = 1
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar -- join to get diagnosis code
        on dx_enc.icd10_code = usnews_metadata_calendar.code
        and lower(usnews_metadata_calendar.question_number) = 'j27'
        and year(enc.encounter_date) = usnews_metadata_calendar.submission_year - 1
where
--    enc.age_months < 24.00
    enc.encounter_type_id in ('101', '50') -- office visit, appointment
    and enc.appointment_status_id in ('2', '6') -- completed, arrived
    and lower(enc.specialty_name) = 'pulmonary'
    and enc.department_id not in ('101022016', '101001610')
group by
    usnews_metadata_calendar.submission_year,
    enc.mrn,
    enc.patient_name,
    enc.encounter_date,
    enc.dob,
    enc.age_months,
    dx_enc.icd10_code,
    enc.pat_key,
    enc.visit_key
),
j27_pat_ind as (
select
    submission_year,
    mrn,
    patient_name,
    pat_key
from j27_cohort
where age_months < 24.00
group by
    submission_year,
    mrn,
    patient_name,
    pat_key
)
-- patients from j27 with at least 1 in-person encounter with Pulm between Oct and Dec
select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.start_date - interval '3 months' as flu_start_date,
    usnews_metadata_calendar.end_date as flu_end_date,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    j27_cohort.mrn,
    j27_cohort.patient_name,
    j27_cohort.dob,
    encounter_all.encounter_date,
    j27_cohort.pat_key,
    j27_cohort.visit_key
from
    j27_pat_ind
    inner join j27_cohort
        on j27_pat_ind.pat_key = j27_cohort.pat_key
        and j27_pat_ind.submission_year = j27_cohort.submission_year
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar -- join to get metadata for j27.1
        on lower(usnews_metadata_calendar.question_number) = 'j27.1'
        and j27_cohort.encounter_date between usnews_metadata_calendar.start_date
        and usnews_metadata_calendar.end_date
    inner join {{ref('encounter_all')}} as encounter_all
        on j27_cohort.visit_key = encounter_all.visit_key
where
    j27_cohort.age_months >= 6
    and encounter_all.telehealth_ind = 0
group by
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    j27_cohort.mrn,
    j27_cohort.patient_name,
    j27_cohort.dob,
    encounter_all.encounter_date,
    j27_cohort.pat_key,
    j27_cohort.visit_key
