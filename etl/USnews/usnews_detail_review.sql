with stage as (
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        num,
        denom,
        primary_key,
        metric_id,
        cpt_code,
        procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        subsequent_date,
        domain,
        subdomain,
        metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        visit_key
    from {{ref('stg_usnews_pulm')}}
    union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        num,
        null as denom,
        cast(primary_key as varchar(30)) as primary_key,
        metric_id,
        cpt_code,
        procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        null as subsequent_date,
        domain,
        subdomain,
        metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        visit_key
    from {{ref('stg_usnews_gi')}}
    union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        mrn as num,
        null as denom,
        cast(mrn as varchar(8)) as primary_key,
        question_number as metric_id,
        null as cpt_code,
        null as procedure_name,
        icd10_code,
        diagnosis_name,
        null as subsequent_date,
        domain,
        subdomain,
        index_date as metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        '0' as visit_key
    from {{ref('stg_neo_f16')}}
    union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        num,
        denom,
        cast(primary_key as varchar(8)) as primary_key,
        metric_id,
        null as cpt_code,
        null as procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        null as subsequent_date,
        domain,
        subdomain,
        index_date as metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        '0' as visit_key
    from {{ref('stg_neo_f31_1')}}
    union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        case when metric_id not like 'h17%' then mrn else num end as num,
        denom,
        cast(primary_key as varchar(8)) as primary_key,
        metric_id,
        cpt_code,
        procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        subsequent_date,
        domain,
        subdomain,
        metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        visit_key
    from {{ref('stg_usnews_neuro')}}
    union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        mrn as num,
        null as denom,
        cast(mrn as varchar(8)) as primary_key,
        metric_id,
        null as cpt_code,
        null as procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        null as subsequent_date,
        domain,
        subdomain,
        index_date as metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        '0' as visit_key
    from {{ref('stg_urology_k08')}}
        union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        num,
        denom,
        primary_key,
        metric_id,
        cpt_code,
        procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        null as subsequent_date,
        'clinical' as domain, --noqa: L029
        null as subdomain,
        index_date as metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        visit_key
    from {{ref('stg_usnews_nephrology')}}
    union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        patient_mrn as mrn,
        patient_name,
        patient_dob as dob,
        index_date,
        num,
        denom,
        primary_key,
        metric_id,
        null as cpt_code,
        null as procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        null as subsequent_date,
        'clinical' as domain, --noqa: L029
        null as subdomain,
        index_date as metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        visit_key
    from {{ref('stg_usnews_cancer_center')}}
    union all
    select
        null as redcap_primary_key,
        submission_year,
        division,
        question_number,
        mrn,
        patient_name,
        dob,
        index_date,
        num,
        denom,
        primary_key,
        metric_id,
        cpt_code,
        procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        null as subsequent_date,
        'clinical' as domain, --noqa: L029
        null as subdomain,
        index_date as metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        visit_key
    from {{ref('stg_usnews_cardiology')}}
    union all
    select
        null as redcap_primary_key,
        stg_usnews_endocrinology.submission_year,
        stg_usnews_endocrinology.division,
        stg_usnews_endocrinology.question_number,
        stg_usnews_endocrinology.mrn,
        stg_usnews_endocrinology.patient_name,
        stg_usnews_endocrinology.dob,
        stg_usnews_endocrinology.index_date,
        stg_usnews_endocrinology.num,
        stg_usnews_endocrinology.denom,
        stg_usnews_endocrinology.primary_key,
        stg_usnews_endocrinology.metric_id,
        stg_usnews_endocrinology.cpt_code,
        stg_usnews_endocrinology.procedure_name,
        null as icd10_code,
        null as diagnosis_name,
        null as subsequent_date,
        stg_usnews_endocrinology.domain, --noqa: L029
        stg_usnews_endocrinology.subdomain,
        stg_usnews_endocrinology.metric_date,
        null as included_ind,
        null as excluded_ind,
        null as last_updated_by_clinician,
        stg_usnews_endocrinology.encounter_key as visit_key
    from
        {{ref ('stg_usnews_endocrinology') }} as stg_usnews_endocrinology
    union all
    select
        usnews_survey_response.redcap_primary_key,
        usnews_survey_response.submission_year,
        usnews_survey_response.division,
        null as question_number,
        usnews_survey_response.mrn,
        usnews_survey_response.patient_name,
        usnews_survey_response.dob,
        usnews_survey_response.index_date,
        null as num,
        null as denom,
        case
            when primary_key like '%_f16%' then cast(substring(primary_key, '1', length(primary_key)-5) as bigint)
            when
                usnews_survey_response.primary_key is null
                or usnews_survey_response.primary_key = usnews_survey_response.mrn
                then stg_patient.pat_key
            else stg_patient.pat_key
        end as primary_key,
        usnews_survey_response.metric_id,
        usnews_survey_response.cpt_code,
        usnews_survey_response.procedure_name,
        usnews_survey_response.icd10_code,
        usnews_survey_response.diagnosis_name,
        usnews_survey_response.subsequent_date,
        null as domain,
        null as subdomain,
        usnews_survey_response.index_date as metric_date,
        case
            when usnews_survey_response.included_ind = 'Yes' then 1
            when usnews_survey_response.included_ind = 'No' then 0
        end as included_ind,
        case
            when usnews_survey_response.included_ind = 'No' then 1
            when usnews_survey_response.included_ind = 'Yes' then 0
        end as excluded_ind,
        usnews_survey_response.last_updated_by_clinician,
        null as visit_key
    from  {{source('ods', 'usnews_survey_response')}} as usnews_survey_response
        left join {{ref('stg_patient')}} as stg_patient
            on usnews_survey_response.mrn = stg_patient.mrn
)

select
    stage.primary_key,
    case
        when redcap_primary_key is null then {{
        dbt_utils.surrogate_key([
            'stage.mrn',
            'stage.submission_year',
            'stage.metric_id',
            'stage.index_date'
        ])
    }}
        else {{
        dbt_utils.surrogate_key([
            'stage.redcap_primary_key'
        ])
    }} end
    as redcap_primary_key,
    stage.submission_year,
    stage.division,
    coalesce(stage.question_number, lookup_usnews_metadata.question_number) as question_number,
    lookup_usnews_metadata.metric_name,
    stage.mrn,
    stage.patient_name,
    stage.dob,
    stage.index_date,
    stage.num,
    stage.denom,
    stage.metric_id,
    group_concat(coalesce(stage.cpt_code, '0')) as cpt_code,
    group_concat(stage.procedure_name) as procedure_name,
    group_concat(stage.icd10_code) as icd10_code,
    group_concat(stage.diagnosis_name) as diagnosis_name,
    case
        when stage.division in ('Gastroenterology', 'Urology', 'NeurologyNeurosurgery')
            or stage.metric_id like 'f16%' then 'operational'
        when stage.division in ('Cancer Center', 'Nephrology') or stage.metric_id like 'f3%' then 'clinical'
        else stage.domain
    end as domain,
    case
        when stage.division in ('Gastroenterology', 'Urology', 'NeurologyNeurosurgery')
            or stage.metric_id like 'f16%' then 'finance'
        else stage.subdomain
    end as subdomain,    
    stage.metric_date,
    stage.subsequent_date,
    lookup_usnews_metadata.num_calculation,
    lookup_usnews_metadata.denom_calculation,
    lookup_usnews_metadata.direction as desired_direction,
    lookup_usnews_metadata.metric_type,
    lookup_usnews_metadata.review_ind,
    lookup_usnews_metadata.submitted_ind,
    lookup_usnews_metadata.scored_ind,
    max(stage.included_ind) as included_ind,
    max(stage.excluded_ind) as excluded_ind,
    stage.last_updated_by_clinician,
    date(lookup_usnews_metadata.usnews_last_updated_date) as usnews_last_updated_date
from
    stage
    inner join {{ref('lookup_usnews_metadata')}} as lookup_usnews_metadata
        on stage.metric_id = lookup_usnews_metadata.metric_id
    left join {{ ref('encounter_inpatient')}} as encounter_inpatient
        on stage.visit_key = encounter_inpatient.visit_key
where
    (stage.submission_year >= lookup_usnews_metadata.submission_start_year
      and stage.submission_year <= lookup_usnews_metadata.submission_end_year)
    or (stage.submission_year >= lookup_usnews_metadata.submission_start_year
      and lookup_usnews_metadata.submission_end_year is null)
group by
    stage.submission_year,
    stage.division,
    stage.question_number,
    lookup_usnews_metadata.question_number,
    lookup_usnews_metadata.metric_name,
    stage.mrn,
    stage.patient_name,
    stage.dob,
    stage.index_date,
    stage.redcap_primary_key,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.hospital_discharge_date,
    stage.num,
    stage.denom,
    stage.primary_key,
    stage.metric_id,
    coalesce(stage.cpt_code, '0'),
    stage.domain,
    stage.subdomain,
    stage.metric_date,
    stage.subsequent_date,
    lookup_usnews_metadata.num_calculation,
    lookup_usnews_metadata.denom_calculation,
    lookup_usnews_metadata.direction,
    lookup_usnews_metadata.metric_type,
    lookup_usnews_metadata.review_ind,
    lookup_usnews_metadata.submitted_ind,
    lookup_usnews_metadata.scored_ind,
    stage.included_ind,
    stage.excluded_ind,
    stage.last_updated_by_clinician,
    lookup_usnews_metadata.usnews_last_updated_date
