with urology_billing as (
    select
        procedure_billing.pat_key,
        procedure_billing.mrn,
        procedure_billing.patient_name,
        procedure_billing.dob,
        procedure_billing.age_years,
        case when procedure_billing.age_years < 21 then 1 else 0 end as under_age_21_ind,
        procedure_billing.service_date,
        procedure_billing.cpt_code,
        procedure_billing.procedure_name,
        procedure_billing.provider_name,
        procedure_billing.provider_specialty,
        procedure_billing.icd10_code,
        procedure_billing.diagnosis_name,
        'Billing' as source
    from
        {{ref('procedure_billing')}} as procedure_billing
    where
        procedure_billing.service_date >= '2017-01-01'
        and procedure_billing.provider_specialty = 'URO'
        and procedure_billing.icd10_code is not null
),
billing_on_code_list as (
    select distinct
        urology_billing.pat_key,
        urology_billing.mrn,
        urology_billing.patient_name,
        urology_billing.dob,
        urology_billing.age_years,
        urology_billing.under_age_21_ind,
        urology_billing.service_date,
        urology_billing.cpt_code,
        urology_billing.procedure_name,
        urology_billing.provider_name,
        urology_billing.provider_specialty,
        urology_billing.icd10_code,
        urology_billing.diagnosis_name,
        urology_billing.source,
        cpt_codes.question as cpt_question,
        cpt_codes.submission_year as cpt_submission_year,
        cpt_codes.inclusion_ind as cpt_inclusion_ind,
        cpt_codes.exclusion_ind as cpt_exclusion_ind,
        cpt_codes.chart_review_ind as cpt_chart_review_ind,
        dx_codes.question as dx_question,
        dx_codes.submission_year as dx_submission_year,
        dx_codes.inclusion_ind as dx_inclusion_ind,
        dx_codes.exclusion_ind as dx_exclusion_ind,
        dx_codes.chart_review_ind as dx_chart_review_ind
    from
        urology_billing
        left join {{ref('urology_usnews_calendar_and_codes')}} as cpt_codes
            on urology_billing.cpt_code = cpt_codes.code
            and urology_billing.service_date between cpt_codes.start_date and cpt_codes.end_date
            and cpt_codes.code_type = 'CPT_CODE'
        left join {{ref('urology_usnews_calendar_and_codes')}} as dx_codes
            on urology_billing.icd10_code = dx_codes.code
            and urology_billing.service_date between dx_codes.start_date and dx_codes.end_date
            and dx_codes.code_type = 'ICD10_CODE'
    where
        cpt_codes.code is not null or dx_codes.code is not null
),
or_log_on_code_list as (
    select distinct
        surgery_procedure.pat_key,
        surgery_procedure.mrn,
        surgery_procedure.patient_name,
        surgery_procedure.dob,
        surgery_encounter.surgery_age_years as age_years,
        case when surgery_encounter.surgery_age_years < 21 then 1 else 0 end as under_age_21_ind,
        surgery_procedure.surgery_date as service_date,
        surgery_procedure.cpt_code,
        surgery_procedure.or_procedure_name as procedure_name,
        surgery_procedure.primary_surgeon as provider_name,
        surgery_procedure.service as provider_specialty,
        diagnosis_encounter_all.icd10_code,
        diagnosis_encounter_all.diagnosis_name,
        'OR Log' as source,
        cpt_codes.question as cpt_question,
        cpt_codes.submission_year as cpt_submission_year,
        cpt_codes.inclusion_ind as cpt_inclusion_ind,
        cpt_codes.exclusion_ind as cpt_exclusion_ind,
        cpt_codes.chart_review_ind as cpt_chart_review_ind,
        dx_codes.question as dx_question,
        dx_codes.submission_year as dx_submission_year,
        dx_codes.inclusion_ind as dx_inclusion_ind,
        dx_codes.exclusion_ind as dx_exclusion_ind,
        dx_codes.chart_review_ind as dx_chart_review_ind
    from
        {{ref('surgery_procedure')}} as surgery_procedure
        inner join {{ref('surgery_encounter')}} as surgery_encounter
            on surgery_procedure.or_key = surgery_encounter.or_key
        left join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on surgery_procedure.visit_key = diagnosis_encounter_all.visit_key
            and diagnosis_encounter_all.visit_diagnosis_ind = 1
        left join {{ref('urology_usnews_calendar_and_codes')}} as cpt_codes
            on surgery_procedure.cpt_code = cpt_codes.code
            and surgery_procedure.surgery_date between cpt_codes.start_date and cpt_codes.end_date
            and cpt_codes.code_type = 'CPT_CODE'
        left join {{ref('urology_usnews_calendar_and_codes')}} as dx_codes
            on diagnosis_encounter_all.icd10_code = dx_codes.code
            and surgery_procedure.surgery_date between dx_codes.start_date and dx_codes.end_date
            and dx_codes.code_type = 'ICD10_CODE'
    where
        surgery_procedure.case_status = 'Completed'
        and surgery_procedure.service = 'Urology'
        and (cpt_codes.code is not null or dx_codes.code is not null)
),
or_log_missing as (
    select
        or_log_on_code_list.pat_key,
        or_log_on_code_list.mrn,
        or_log_on_code_list.patient_name,
        or_log_on_code_list.dob,
        or_log_on_code_list.age_years,
        or_log_on_code_list.under_age_21_ind,
        or_log_on_code_list.service_date,
        or_log_on_code_list.cpt_code,
        or_log_on_code_list.procedure_name,
        or_log_on_code_list.provider_name,
        or_log_on_code_list.provider_specialty,
        or_log_on_code_list.icd10_code,
        or_log_on_code_list.diagnosis_name,
        or_log_on_code_list.source,
        or_log_on_code_list.cpt_question,
        or_log_on_code_list.cpt_submission_year,
        or_log_on_code_list.cpt_inclusion_ind,
        or_log_on_code_list.cpt_exclusion_ind,
        or_log_on_code_list.cpt_chart_review_ind,
        or_log_on_code_list.dx_question,
        or_log_on_code_list.dx_submission_year,
        or_log_on_code_list.dx_inclusion_ind,
        or_log_on_code_list.dx_exclusion_ind,
        or_log_on_code_list.dx_chart_review_ind
    from
        or_log_on_code_list
        left join billing_on_code_list as cpt_check
            on or_log_on_code_list.pat_key = cpt_check.pat_key
            and or_log_on_code_list.service_date --one day buffer
                between cpt_check.service_date - 1
                    and cpt_check.service_date + 1
            and or_log_on_code_list.cpt_question || or_log_on_code_list.cpt_submission_year
                = cpt_check.cpt_question || cpt_check.cpt_submission_year
        left join billing_on_code_list as dx_check
            on or_log_on_code_list.pat_key = dx_check.pat_key
            and or_log_on_code_list.service_date --one day buffer
                between dx_check.service_date - 1
                    and dx_check.service_date + 1
            and or_log_on_code_list.dx_question || or_log_on_code_list.dx_submission_year
                = dx_check.dx_question || dx_check.dx_submission_year
    where
        cpt_check.pat_key is null and dx_check.pat_key is null
)
select distinct
        billing_on_code_list.pat_key,
        billing_on_code_list.mrn,
        billing_on_code_list.patient_name,
        billing_on_code_list.dob,
        billing_on_code_list.age_years,
        billing_on_code_list.under_age_21_ind,
        billing_on_code_list.service_date,
        billing_on_code_list.cpt_code,
        billing_on_code_list.procedure_name,
        billing_on_code_list.provider_name,
        billing_on_code_list.provider_specialty,
        billing_on_code_list.icd10_code,
        billing_on_code_list.diagnosis_name,
        billing_on_code_list.source,
        billing_on_code_list.cpt_question,
        billing_on_code_list.cpt_submission_year,
        billing_on_code_list.cpt_inclusion_ind,
        billing_on_code_list.cpt_exclusion_ind,
        billing_on_code_list.cpt_chart_review_ind,
        billing_on_code_list.dx_question,
        billing_on_code_list.dx_submission_year,
        billing_on_code_list.dx_inclusion_ind,
        billing_on_code_list.dx_exclusion_ind,
        billing_on_code_list.dx_chart_review_ind
from
    billing_on_code_list
union all
select distinct
        or_log_missing.pat_key,
        or_log_missing.mrn,
        or_log_missing.patient_name,
        or_log_missing.dob,
        or_log_missing.age_years,
        or_log_missing.under_age_21_ind,
        or_log_missing.service_date,
        or_log_missing.cpt_code,
        or_log_missing.procedure_name,
        or_log_missing.provider_name,
        or_log_missing.provider_specialty,
        or_log_missing.icd10_code,
        or_log_missing.diagnosis_name,
        or_log_missing.source,
        or_log_missing.cpt_question,
        or_log_missing.cpt_submission_year,
        or_log_missing.cpt_inclusion_ind,
        or_log_missing.cpt_exclusion_ind,
        or_log_missing.cpt_chart_review_ind,
        or_log_missing.dx_question,
        or_log_missing.dx_submission_year,
        or_log_missing.dx_inclusion_ind,
        or_log_missing.dx_exclusion_ind,
        or_log_missing.dx_chart_review_ind
from
    or_log_missing
