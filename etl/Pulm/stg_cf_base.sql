/*
 * File Name: cf_ogtt_age_cohort.sql
 *
 * Author: Reiyuan Chiu
 *
 * Granularity: Patient level (mrn)
 *
 * Output: Return Cystic Fibrosis patients on the Healthy Planet patient list and
 * if and when (if applicable) they had an Oral Glucose Tolerance Test (OGTT) performed.
 * Excluding patients who have previously had a diagnosis of Cystic Fibrosis-related
 * Diabetes (CFRD) or a lung transplant.
 *
 * OGTTs are currently only able to be tracked through orders or procedures made within
 * the CHOP network. OGTTs with results scanned into the Media tab in Epic only include
 * the date they were scanned in rather than the testing date and are therefore omitted.
 *
 * Last Changed: 6/26/2023
 * */

-- Pulling out all CF patients who are on the CF Healthy Planet report
select
    enc.pat_key,
    min(enc.age_years) as age_years,
    extract(year from enc.encounter_date) as cy
from {{ref('encounter_specialty_care')}} as enc
-- Filter only to visits from active patients on the CF registry
inner join {{ref('epic_registry_membership_history')}} as registry
    on enc.mrn = registry.mrn
    and registry.epic_registry_id = '100447' -- Cystic Fibrosis registry
    and registry.current_record_ind = 1 -- Only pull most recent membership records for patients
    and registry.enrollment_end is null -- Only pull patients who do not have an end date on the registry
-- Looking for CF diagnoses in visit diagnoses and problem lists
inner join {{ref('diagnosis_encounter_all')}} as dx
    on enc.visit_key = dx.visit_key
    and dx.icd10_code like 'E84%'
    and (dx.problem_list_ind = 1
        or dx.visit_diagnosis_ind = 1)
-- the below 2 SDEs track whether the patients should be excluded for other reasons
left join {{ref('smart_data_element_all')}} as sde
    on enc.pat_key = sde.pat_key
    and sde.concept_id in ('CHOP#6872', -- Patients excluded for CRMS, CFTR, or lacking CF diagnosis
                            'CHOP#6862') -- Excludes patients not followed by CF clinic due to transfers, etc.
-- change time period below for different requests
where enc.encounter_date between date('2022-01-01') and now()
group by
    enc.pat_key,
    cy
-- exclude patients who have the exclusionary SDEs populated
having max(sde.element_value) is null
    and min(enc.age_years) >= 8
