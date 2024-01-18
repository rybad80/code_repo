{%- set f16_questions = [
    "f16a",
    "f16b",
    "f16d",
    "f16e",
    "f16f",
    "f16g",
    "f16h",
    "f16i",
    "f16k"
] -%}


select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_billing.division,
    usnews_billing.question_number,
    usnews_billing.metric_name,
    usnews_billing.submission_year,
    usnews_billing.mrn,
    usnews_billing.patient_name,
    usnews_billing.dob,
    usnews_billing.icd10_code,
    usnews_billing.diagnosis_name,
    usnews_billing.service_date as index_date,
    lookup_usnews_metadata.num_calculation,
    lookup_usnews_metadata.denom_calculation,
    lookup_usnews_metadata.direction

from
    {{ ref('usnews_billing') }} as usnews_billing
    inner join {{ ref('neo_nicu_episode') }} as neo_nicu_episode
        on neo_nicu_episode.visit_key = usnews_billing.visit_key
    inner join {{ ref('lookup_usnews_metadata') }} as lookup_usnews_metadata
        on lookup_usnews_metadata.question_number = usnews_billing.question_number

where
    {%- for question in f16_questions -%}
        {% if not loop.first %} or {%- endif %} usnews_billing.question_number = '{{ question }}'
    {% endfor -%}


union all


select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_clinical.division,
    usnews_clinical.question_number,
    usnews_clinical.metric_name,
    usnews_clinical.submission_year,
    usnews_clinical.mrn,
    usnews_clinical.patient_name,
    usnews_clinical.dob,
    usnews_clinical.icd10_code,
    usnews_clinical.diagnosis_name,
    usnews_clinical.encounter_date as index_date,
    lookup_usnews_metadata.num_calculation,
    lookup_usnews_metadata.denom_calculation,
    lookup_usnews_metadata.direction

from
    {{ ref('usnews_clinical') }} as usnews_clinical
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on encounter_inpatient.csn = usnews_clinical.csn
    inner join {{ ref('neo_nicu_episode') }} as neo_nicu_episode
        on neo_nicu_episode.visit_key = encounter_inpatient.visit_key
    inner join {{ ref('lookup_usnews_metadata') }} as lookup_usnews_metadata
        on lookup_usnews_metadata.question_number = usnews_clinical.question_number

where
    {%- for question in f16_questions -%} 
        {% if not loop.first %} or {%- endif %} usnews_clinical.question_number = '{{ question }}'
    {% endfor -%}


union all


select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    neo_nicu_cohort_group.mrn,
    neo_nicu_cohort_group.patient_name,
    neo_nicu_cohort_group.dob,
    null as icd10_code,
    null as diagnosis_name,
    neo_nicu_cohort_group.cohort_group_enter_date as index_date,
    lookup_usnews_metadata.num_calculation,
    lookup_usnews_metadata.denom_calculation,
    lookup_usnews_metadata.direction

from
    {{ ref('neo_nicu_cohort_group')}} as neo_nicu_cohort_group
    inner join {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
        on date(neo_nicu_cohort_group.cohort_group_enter_date)
            between usnews_metadata_calendar.start_date
            and usnews_metadata_calendar.end_date
    inner join {{ ref('lookup_usnews_metadata') }} as lookup_usnews_metadata
        on lookup_usnews_metadata.question_number = usnews_metadata_calendar.question_number

where
    usnews_metadata_calendar.question_number = 'f16j'
