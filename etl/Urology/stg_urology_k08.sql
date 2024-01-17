with metadata as (
    select
        usnews_metadata_calendar.*
    from
        {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
    where
        usnews_metadata_calendar.question_number = 'K08'
)

select
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    encounter_specialty_care.patient_name,
    encounter_specialty_care.mrn,
    encounter_specialty_care.dob,
    encounter_specialty_care.sex,
    metadata.submission_year,
    metadata.division,
    metadata.question_number,
    metadata.metric_name,
    metadata.metric_id,
    min(encounter_specialty_care.encounter_date) as index_date
from
    {{ ref('encounter_specialty_care') }} as encounter_specialty_care
    cross join metadata
    inner join {{ ref('lookup_usnews_metadata') }} as lookup_usnews_metadata
            on metadata.question_number = lookup_usnews_metadata.question_number
where
    encounter_specialty_care.specialty_name = 'UROLOGY'
    and encounter_specialty_care.encounter_date between metadata.start_date and metadata.end_date
    and encounter_specialty_care.age_years between metadata.age_gte and metadata.age_lt
group by
    encounter_specialty_care.patient_name,
    encounter_specialty_care.mrn,
    encounter_specialty_care.dob,
    encounter_specialty_care.sex,
    metadata.submission_year,
    metadata.division,
    metadata.question_number,
    metadata.metric_name,
    metadata.metric_id
