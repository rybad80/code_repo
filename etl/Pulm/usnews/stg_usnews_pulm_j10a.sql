-- primary key: pat_key || visit_key
select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    usnews_metadata_calendar.metric_name,
    base.pat_key,
    base.visit_key,
    base.mrn,
    base.patient_name,
    base.dob,
    base.age_years,
    base.csn,
    base.encounter_date,
    base.sex,
    base.provider_name,
    base.provider_specialty,
    base.department_name,
    base.department_specialty,
    base.encounter_type,
    base.visit_type,
    base.appointment_status,
    base.icd10_code,
    base.diagnosis_name
from
    {{ref('stg_usnews_pulm_asthma_base')}} as base
    left join {{ref('stg_usnews_pulm_asthma_exclusion')}} as exc
        on base.pat_key = exc.pat_key
        and (
            exc.date_cutoff_ind = 0 -- patients with conditions regardless of time when first diagnosed
            or (
                exc.date_cutoff_ind = 1
                and exc.index_date <= base.encounter_date -- patients with conditions prior to asthma encounter
                )
            )
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on base.icd10_code = usnews_metadata_calendar.code
        and lower(usnews_metadata_calendar.metric_id) = 'j10a'
        and (base.encounter_date >= usnews_metadata_calendar.start_date
        and base.encounter_date <= usnews_metadata_calendar.end_date)
        and (floor(base.age_years) >= usnews_metadata_calendar.age_gte
        and floor(base.age_years) <= usnews_metadata_calendar.age_lt)
where
    base.primary_dx_ind = 1
    and exc_cohort is null
