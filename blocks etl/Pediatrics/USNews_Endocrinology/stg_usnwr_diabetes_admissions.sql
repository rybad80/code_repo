with admissions as ( --region
    select
        encounter_all.patient_key,
        max(encounter_all.encounter_date) as last_encounter_date,
        max(encounter_all.inpatient_ind) as ip_admissions_ind,
        --, max(ed_ind) as ed_ind
        max(case when (encounter_all.ed_ind = 1
            or lower(encounter_all.department_name) like '%urgent care center%')
                and encounter_all.inpatient_ind = 0
            then 1 else 0
        end) as diabetes_ed_urgent_ind
    from
        {{ref('stg_usnwr_diabetes_type_cohort')}} as stg_usnwr_diabetes_type_cohort
        inner join {{ref('encounter_all')}} as encounter_all
            on encounter_all.patient_key = stg_usnwr_diabetes_type_cohort.patient_key
        left join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on diagnosis_encounter_all.visit_key = encounter_all.visit_key
    where
        year(encounter_all.encounter_date) = '2023'
        and (encounter_all.inpatient_ind = 1 or encounter_all.ed_ind = 1
            or lower(encounter_all.department_name) like '%urgent care center%')
        and diagnosis_encounter_all.hsp_acct_admit_primary_ind = 1 -- admit dx
        and diagnosis_encounter_all.icd10_code in (
        --region Type 1 diabetes related causes from C28.1c code_list
            'E10.10',
            'E10.11',
            'E10.641',
            'E10.649',
        --,'E10.65' --removed because this is hyperglycemia not hypo
        --end region
        --region Type 2 diabetes related causes from C28.1d code_list
            'E11.00',
            'E11.01',
            'E11.641',
            'E11.649'
            --,'E11.65'--removed because this is hyperglycemia not hypo
        --end region
        )
    group by
        encounter_all.patient_key
),

flowsheet_dx as (
    select
        smart_data_element_all.pat_key,
        smart_data_element_all.mrn,
        cast(smart_data_element_all.concept_id as varchar(225)) as concept_id,
        row_number() over(
            partition by
                smart_data_element_all.pat_key,
                smart_data_element_all.concept_id
            order by
                smart_data_element_all.encounter_date desc
        ) as row_num_dka,
        case
            when smart_data_element_all.concept_id in ('CHOP#7529', 'CHOP#7526')
                and smart_data_element_all.element_value = 'Yes'
            then '1' else '0'
        end as seen_in_last_year_ind
    from
        {{ref('smart_data_element_all')}} as smart_data_element_all
        inner join {{ref('stg_usnwr_diabetes_type_cohort')}} as stg_usnwr_diabetes_type_cohort
            on smart_data_element_all.pat_key = stg_usnwr_diabetes_type_cohort.pat_key
                and smart_data_element_all.encounter_date
                    between stg_usnwr_diabetes_type_cohort.start_date and stg_usnwr_diabetes_type_cohort.end_date
    where
        smart_data_element_all.concept_id in ('CHOP#7529', 'CHOP#7526') --IP and ED DKA Admissions in the last year
    union all
    select
        flowsheet_all.pat_key,
        flowsheet_all.mrn,
        cast(flowsheet_all.flowsheet_id as varchar(225)) as concept_id,
        row_number() over(
            partition by
                flowsheet_all.pat_key,
                flowsheet_all.flowsheet_id
            order by
                flowsheet_all.encounter_date desc
        ) as row_num_dka,
        case
            when flowsheet_all.flowsheet_id in ('15772', '15777')
                and flowsheet_all.meas_val = 'Yes'
            then '1' else '0'
        end as seen_in_last_year_ind
    from
        {{ref('flowsheet_all')}} as flowsheet_all
        inner join {{ref('stg_usnwr_diabetes_type_cohort')}} as stg_usnwr_diabetes_type_cohort
            on flowsheet_all.pat_key = stg_usnwr_diabetes_type_cohort.pat_key
                and flowsheet_all.encounter_date
                    between stg_usnwr_diabetes_type_cohort.start_date and stg_usnwr_diabetes_type_cohort.end_date
    where
        --ED presentation for ketones since last visit; Inpatient DKA admission since last visit
        flowsheet_all.flowsheet_id in ('15772', '15777')
)

select
    stg_usnwr_diabetes_type_cohort.patient_key,
    stg_usnwr_diabetes_type_cohort.mrn,
    coalesce(admissions.ip_admissions_ind, 0) as ip_admissions_ind,
    coalesce(admissions.diabetes_ed_urgent_ind, 0) as diabetes_ed_urgent_ind,
    max(case
        when flowsheet_dx.concept_id in ('15777', 'CHOP#7529')
        then flowsheet_dx.seen_in_last_year_ind
    end) as prov_report_ip_ind,
    max(case
        when flowsheet_dx.concept_id in ('15772', 'CHOP#7526')
        then flowsheet_dx.seen_in_last_year_ind
    end) as prov_report_ed_ind
from
    {{ref('stg_usnwr_diabetes_type_cohort')}} as stg_usnwr_diabetes_type_cohort
    left join admissions
        on stg_usnwr_diabetes_type_cohort.patient_key = admissions.patient_key
    left join flowsheet_dx
        on stg_usnwr_diabetes_type_cohort.pat_key = flowsheet_dx.pat_key
            and flowsheet_dx.row_num_dka = '1'
group by
    stg_usnwr_diabetes_type_cohort.patient_key,
    stg_usnwr_diabetes_type_cohort.mrn,
    coalesce(admissions.ip_admissions_ind, 0),
    coalesce(admissions.diabetes_ed_urgent_ind, 0)
