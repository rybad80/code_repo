with cgm_procedures as (
    select
        procedure_order_clinical.pat_key,
        procedure_order_clinical.encounter_key,
        procedure_order_clinical.encounter_date,
        case
            when lower(procedure_order_clinical.cpt_code) = 'endocgm001'
            then 1 else 0
        end as cgm_interp_procedure
    from
        {{ref('procedure_order_clinical')}} as procedure_order_clinical
        inner join {{ref('stg_usnwr_diabetes_calendar')}} as stg_usnwr_diabetes_calendar
            on procedure_order_clinical.encounter_key = stg_usnwr_diabetes_calendar.encounter_key
                and procedure_order_clinical.encounter_date
                    between stg_usnwr_diabetes_calendar.start_date and stg_usnwr_diabetes_calendar.end_date
    where
        lower(procedure_order_clinical.cpt_code) = 'endocgm001'
),

recent_cgm_status as (
    select
        diabetes_tech_use.patient_key,
        diabetes_tech_use.final_encounter_date,
        diabetes_tech_use.has_cgm,
        row_number() over(
            partition by
                diabetes_tech_use.patient_key
            order by
                diabetes_tech_use.diabetes_reporting_month desc
        ) as recent_cgm_status
    from
        {{ref('diabetes_tech_use')}} as diabetes_tech_use
    where
        (diabetes_tech_use.has_cgm is not null
        or (diabetes_tech_use.has_cgm is not null
            and diabetes_tech_use.cgm_type_rn = '1'))
),

cohort_final as (
    select
        stg_usnwr_diabetes_primary_pop.primary_key,
        stg_usnwr_diabetes_primary_pop.mrn,
        stg_usnwr_diabetes_primary_pop.patient_name,
        stg_usnwr_diabetes_primary_pop.dob,
        stg_usnwr_diabetes_primary_pop.diabetes_type_12,
        stg_usnwr_diabetes_primary_pop.insurance_status,
        max(stg_usnwr_diabetes_primary_pop.metric_date) as last_cgm_interpretated_date,
        count(distinct case
            when (cgm_interpreted_today = 'Yes'
                or current_cgm_range_analysis_ind is not null
                or cgm_procedures.cgm_interp_procedure = '1'
                and stg_usnwr_diabetes_primary_pop.diabetes_type_12 = 'Type 1')
            then stg_usnwr_diabetes_primary_pop.encounter_key
        end) as cgm_interpreted,
        max(case
            when (cgm_interpreted_today = 'Yes'
                and stg_usnwr_diabetes_primary_pop.diabetes_type_12 = 'Type 1')
            then 1 else 0
        end) as cgm_used_ind,
        recent_cgm_status.has_cgm as cgm_status,
        stg_usnwr_diabetes_primary_pop.submission_year
    from
        {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
        inner join {{ref('diabetes_tech_use')}} as diabetes_tech_use
            on stg_usnwr_diabetes_primary_pop.primary_key = diabetes_tech_use.patient_key
                and stg_usnwr_diabetes_primary_pop.diabetes_type_12 = 'Type 1'
                and diabetes_tech_use.final_encounter_date
                    between stg_usnwr_diabetes_primary_pop.start_date and stg_usnwr_diabetes_primary_pop.end_date
        left join cgm_procedures
            on stg_usnwr_diabetes_primary_pop.pat_key = cgm_procedures.pat_key
        left join recent_cgm_status
            on stg_usnwr_diabetes_primary_pop.primary_key = recent_cgm_status.patient_key
                and recent_cgm_status.recent_cgm_status = '1'
    where
        stg_usnwr_diabetes_primary_pop.diabetes_type_12 = 'Type 1'
    group by
        stg_usnwr_diabetes_primary_pop.primary_key,
        stg_usnwr_diabetes_primary_pop.mrn,
        stg_usnwr_diabetes_primary_pop.patient_name,
        stg_usnwr_diabetes_primary_pop.dob,
        stg_usnwr_diabetes_primary_pop.diabetes_type_12,
        stg_usnwr_diabetes_primary_pop.insurance_status,
        recent_cgm_status.has_cgm,
        stg_usnwr_diabetes_primary_pop.submission_year
)

select
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    cohort_final.primary_key as patient_key,
    cohort_final.mrn,
    cohort_final.patient_name,
    cohort_final.dob,
    cohort_final.diabetes_type_12,
    cohort_final.insurance_status,
    cohort_final.last_cgm_interpretated_date,
    cohort_final.cgm_interpreted,
    case
        when cohort_final.cgm_interpreted != '0'
        then 1
        when cohort_final.cgm_status like 'Yes%'
        then 1
        else cohort_final.cgm_used_ind
    end as cgm_usage_ind,
    cohort_final.cgm_status,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    case
        when usnews_metadata_calendar.metric_id = 'c32.1a'
            and lower(cohort_final.insurance_status) = 'private'
            and cgm_usage_ind = '1'
        then cohort_final.primary_key
        when usnews_metadata_calendar.metric_id = 'c32.1b'
            and lower(cohort_final.insurance_status) = 'non-private'
            and cgm_usage_ind = '1'
        then cohort_final.primary_key
    end as num,
    cohort_final.submission_year
from
    cohort_final
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on lower(usnews_metadata_calendar.question_number) = 'c32.1'
where
    num is not null
