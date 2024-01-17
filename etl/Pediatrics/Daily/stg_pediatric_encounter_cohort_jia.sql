{{ config(meta = {
    'critical': true
}) }}

/*Pull Rheumatology JIA cohort*/
with
jia_visit as (--region pulling at patients with an encounter with a JIA ICD10 code
    select
        encounter_specialty_care.pat_key,
        min(encounter_specialty_care.encounter_date) as min_date
    from
        {{ref('encounter_specialty_care')}} as encounter_specialty_care
        inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on encounter_specialty_care.visit_key = diagnosis_encounter_all.visit_key
    where
        (diagnosis_encounter_all.icd10_code like 'M08.%' or diagnosis_encounter_all.icd10_code = 'L40.54')
        and encounter_specialty_care.department_name like '%RHEUMATOLOGY%'
    group by
        encounter_specialty_care.pat_key
), -- end region

smartform as (-- region JIA ILAR Smartform
    select
        smart_data_element_all.pat_key as pat_key,
        min(smart_data_element_all.entered_date) as min_date
    from
        {{ref('smart_data_element_all')}} as smart_data_element_all
    where
        lower(smart_data_element_all.concept_id) = 'epic#31000019846'
    group by
        smart_data_element_all.pat_key
), -- end region

redcap as (-- region old Redcap data
    select
        stg_rheumatology_jia_redcap.pat_key as pat_key,
        min(stg_rheumatology_jia_redcap.contact_dt) as min_date
    from
        {{ref('stg_rheumatology_jia_redcap')}} as stg_rheumatology_jia_redcap
    group by
        stg_rheumatology_jia_redcap.pat_key
), -- end region
combine as (--region Union Tables
    select
        pat_key,
        min_date
    from
        smartform
    union all
    select
        pat_key,
        min_date
    from
        redcap
    union all
    select
        pat_key,
        min_date
    from
        jia_visit
    where
        min_date > '2016-01-01' --only want patients who started care in 2016
), -- end region
jia_patient as (--region new JIA Patient
    select
        pat_key,
        min(min_date) as first_date
    from
        combine
    group by
        pat_key
), -- end region
rheum_visit as (--region
    select
        encounter_specialty_care.visit_key,
        encounter_specialty_care.pat_key,
        encounter_specialty_care.mrn,
        encounter_specialty_care.encounter_date,
        encounter_specialty_care.csn
    from
        {{ref('encounter_specialty_care')}} as encounter_specialty_care
    where
        encounter_specialty_care.department_id in (
            89486032,   --'wood rheumatology'
            101012119,  --'bwv rheumatology'
            101022047,  --'pnj rheumatology'
            101012162   --'bgr lupus multid cln'
            )
        and encounter_specialty_care.encounter_date between '2016-01-01' and current_date
) -- end region

select
    rheum_visit.visit_key,
    rheum_visit.pat_key,
    rheum_visit.mrn,
    rheum_visit.csn,
    rheum_visit.encounter_date,
    1 as jia_ind
from
    jia_patient
    inner join rheum_visit
        on jia_patient.pat_key = rheum_visit.pat_key
where
    jia_patient.first_date <= rheum_visit.encounter_date
