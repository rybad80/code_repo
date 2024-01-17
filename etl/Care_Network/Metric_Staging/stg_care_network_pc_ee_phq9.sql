/*
    stg_care_network_pc_ee_phq9.sql
    staging table for PHQ-9 Screening Rate for PC Scorecard in PC Service Line Dashboard
*/
-- Identify patients who refused to complete PHQ-9
with refused_questionnaire_visits as (
    select
        smart_data_element_all.pat_key,
        smart_data_element_all.visit_key,
        smart_data_element_all.encounter_date,
        element_value as phq9_refusal_reason,
        1 as refused_ind
    from {{ref('smart_data_element_all')}} as smart_data_element_all
        inner join {{source('cdw', 'visit')}} as visit on visit.visit_key = smart_data_element_all.visit_key
    where concept_id = 'CHOP#6732' -- concept_description = 'PHQ-9 REASON FOR DECLINED'
        and visit.eff_dt >= '2020-07-01'
        and visit.age_days between 4383 and 8034 --patient was between 4383-8034 days old
    group by smart_data_element_all.pat_key,
        smart_data_element_all.visit_key,
        smart_data_element_all.encounter_date,
        phq9_refusal_reason
),
/*
    from visit_form data, identify patient-visit pairs for which
    a PHQ9 screening was completed
*/
completed_questionnaire_visits as (
    select
        stg_encounter.pat_key,
        stg_encounter.visit_key,
        1 as questionnaire_ind
    from {{ref('stg_encounter_form_answer')}}  as stg_encounter_form_answer
    left join {{ref('stg_encounter')}} as stg_encounter on
        stg_encounter_form_answer.encounter_key = stg_encounter.encounter_key
    where stg_encounter_form_answer.form_id = '100101'  -- relevant form id for phq9
        and stg_encounter.eff_dt >= '2020-07-01'
    group by stg_encounter.pat_key, stg_encounter.visit_key
)
/*
    combine visit-level data for well visits that occurred on or after 2020-07-01
    and the patient was 13-18 years old at the time of visit
    and join with patient-visit pairs where PHQ-9 screener was completed and/or refused
*/
select
    encounter_primary_care.visit_key,
    encounter_primary_care.csn,
    encounter_primary_care.pat_key,
    encounter_primary_care.mrn,
    encounter_primary_care.dob,
    visit.age,
    visit.age_days,
    encounter_primary_care.encounter_date,
    encounter_primary_care.dept_key,
    encounter_primary_care.department_name,
    encounter_primary_care.prov_key,
    encounter_primary_care.provider_name,
    provider.prov_type,
    visit.appt_visit_type_key,
    master_visit_type.visit_type_nm,
    encounter_primary_care.well_visit_ind,
    completed_questionnaire_visits.pat_key as questionnaire_pat_key,
    refused_questionnaire_visits.pat_key as refused_pat_key,
    case when completed_questionnaire_visits.questionnaire_ind = 1 then 1 else 0 end as questionnaire_ind,
    case when refused_questionnaire_visits.refused_ind = 1 then 1 else 0 end as refused_ind,
    refused_questionnaire_visits.phq9_refusal_reason
from {{source('cdw', 'visit')}} as visit
    inner join {{ref('encounter_primary_care')}} as encounter_primary_care
        on visit.visit_key = encounter_primary_care.visit_key
    left join {{source('cdw', 'master_visit_type')}} as master_visit_type
        on visit.appt_visit_type_key = master_visit_type.visit_type_key
    left join {{source('cdw', 'provider')}} as provider
        on visit.visit_prov_key = provider.prov_key
    left join completed_questionnaire_visits
        on visit.visit_key = completed_questionnaire_visits.visit_key
            and visit.pat_key = completed_questionnaire_visits.pat_key
    left join refused_questionnaire_visits
        on visit.visit_key = refused_questionnaire_visits.visit_key
            and visit.pat_key = refused_questionnaire_visits.pat_key
where
    encounter_primary_care.encounter_date >= '2020-07-01'
    and encounter_primary_care.age_days between 4383 and 8034 --patient was between 4383-8034 days old
    and visit.appt_stat = 'COMPLETED' -- visit occured
    and (encounter_primary_care.well_visit_ind = 1 -- screenings occur at adolescent well visits
    -- well visit types 
    --(capture some extra combo sick/well visits that were billed as sick and won't show up in well_visit_ind)
    or master_visit_type.visit_type_id in (
        '1307', -- WELL CHILD VISIT
        '1312', -- NEWBORN WELL
        '1335', -- NEW PATIENT WELL
        '1358', -- WELL CHILD VISIT
        '1363', -- NEWBORN WELL
        '1372', -- NEWBORN-18MO WELL
        '1373', -- NEWBORN-18MO WELL
        '1379', -- TEEN WELL VISIT
        '1413', -- INFANT WELL 1 MO - 6MO
        '2132', -- WELL CHILD - 1 YR OLD
        '2133', -- WELL CHILD - 11 YR OLD
        '2134', -- WELL CHILD-15 YR OLD
        '2158', -- MEDICAL HOME WELL VISIT
        '2368', -- WELL INFANT PATIENT
        '2902', -- WELL CHILD - 4 YEARS OLD
        '2908', -- WELL 6YRS & UNDER
        '2909', -- WELL 7YRS & OLDER
        '2910', -- WELL TEEN 13YRS & OLDER
        '2913', -- NEW PATIENT WELL
        '2921', -- WELL INFANT PATIENT 2M-5Y
        '2922', -- WELL ESTABLISH PATIENT 6Y-10Y
        '2923', -- WELL ADOLESCENT 11Y-18Y
        '2925', -- WELL INFANT PATIENT 2MO- 3YR
        '2931', -- WELL 30 MIN
        '2932', -- WELL INFANT PATIENT 4Y
        '2933', -- WELL INFANT PATIENT 5Y
        '2940', -- WELL CHILD 15 MONTH
        '4014', -- ADOLESCENT WELL VISIT
        '4015', -- NEW PATIENT VISIT 40 MIN
        '4016', -- NEWBORN-40 MIN
        '4019', -- WELL 20 MIN ADO 40 MIN
        '4022', -- WELL 20 MIN/ADO 40 MIN
        '4024', -- ADOLESCENT WELL VISIT
        '9902') -- MYCHOP WELL CHILD VISIT
        )
