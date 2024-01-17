with stage as (
    select distinct
        usnwr_metadata_calendar.submission_year,
        usnwr_metadata_calendar.metric_id,
        case
            when lower(usnwr_metadata_calendar.code_type) = 'icd10_code'
                and usnwr_metadata_calendar.inclusion_ind = 1
                then 1
            else 0
        end as dx_inclusion_ind,
        case
            when lower(usnwr_metadata_calendar.code_type) = 'icd10_code'
                and usnwr_metadata_calendar.exclusion_ind = 1
                then 1
            else 0
        end as dx_exclusion_ind,
        stg_encounter.pat_key,
        stg_encounter.mrn,
        stg_encounter.patient_name,
        stg_encounter.csn,
        stg_encounter.encounter_date,
        stg_encounter.dob,
        stg_encounter.age_years,
        patient.death_dt as death_date,
        case
            when stg_encounter_inpatient.visit_key is not null
            then 1
            else 0
        end as inpatient_ind,
        stg_encounter.encounter_type_id,
        stg_encounter.encounter_type,
        stg_encounter.appointment_status_id,
        stg_encounter.appointment_status,
        stg_encounter.department_id,
        stg_encounter.department_name,
        provider.prov_id as provider_id,
        initcap(provider.full_nm) as provider_name,
        department.specialty as department_specialty,
        diagnosis_encounter_all.icd10_code,
        diagnosis_encounter_all.diagnosis_name,
        diagnosis_encounter_all.visit_diagnosis_ind,
        coalesce(diagnosis_encounter_all.visit_diagnosis_seq_num, -1) as visit_diagnosis_seq_num
    from
        {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    inner join
        {{ ref('usnews_metadata_calendar') }} as usnwr_metadata_calendar
        on diagnosis_encounter_all.icd10_code = usnwr_metadata_calendar.code
            and lower(usnwr_metadata_calendar.code_type) = 'icd10_code'
            and diagnosis_encounter_all.encounter_date between usnwr_metadata_calendar.start_date
            and usnwr_metadata_calendar.end_date
    inner join
        {{ ref('stg_encounter')}} as stg_encounter
        on diagnosis_encounter_all.visit_key = stg_encounter.visit_key
    inner join
        {{source('cdw', 'department')}} as department
        on stg_encounter.department_id = department.dept_id
    inner join
        {{ source('cdw', 'patient')}} as patient
        on diagnosis_encounter_all.pat_key = patient.pat_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
    where
        stg_encounter.age_years between usnwr_metadata_calendar.age_gte and usnwr_metadata_calendar.age_lt
        and stg_encounter.appointment_status_id in (2, 6, -2)
        and stg_encounter.encounter_date >= '2017-01-01'
        and stg_encounter.encounter_date < current_date
        and {{ limit_dates_for_dev(ref_date = 'stg_encounter.encounter_date') }}
)
select
    stage.*,
    lookup_usnews_metadata.division,
    lookup_usnews_metadata.question_number,
    lookup_usnews_metadata.metric_name,
    lookup_usnews_metadata.num_calculation,
    lookup_usnews_metadata.denom_calculation,
    lookup_usnews_metadata.direction,
    case when lookup_usnews_metadata.keep_criteria = 'dx'
        and stage.dx_inclusion_ind = 1 then 1
        else 0 end as keep_ind,
    lookup_usnews_metadata.review_ind,
    lookup_usnews_metadata.submitted_ind,
    lookup_usnews_metadata.scored_ind
from stage
    inner join {{ref('lookup_usnews_metadata')}} as lookup_usnews_metadata
        on lookup_usnews_metadata.metric_id = stage.metric_id
where
    (lower(stage.department_specialty) = lower(lookup_usnews_metadata.specialty)
        or specialty is null)
    and keep_ind = 1
