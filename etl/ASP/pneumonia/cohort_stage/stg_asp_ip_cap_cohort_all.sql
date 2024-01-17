--all encounters where patient had relevant ICD-10 Codes
with cap_diagnoses as (
    select
        diagnosis_encounter_all.visit_key,
        diagnosis_encounter_all.pat_key,
        max(lookup_asp_inpatient_cap_diagnoses.cohort_ind) as cohort_ind,
        max(lookup_asp_inpatient_cap_diagnoses.ci_ind) as ci_ind,
        max(lookup_asp_inpatient_cap_diagnoses.complicated_pneumonia_ind) as complicated_pneumonia_ind,
        max(lookup_asp_inpatient_cap_diagnoses.other_dx_ind) as other_dx_ind
    from
        {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        inner join {{ref('lookup_asp_inpatient_cap_diagnoses')}} as lookup_asp_inpatient_cap_diagnoses
            on diagnosis_encounter_all.icd10_code = lookup_asp_inpatient_cap_diagnoses.icd10_code
    where
        diagnosis_encounter_all.visit_diagnosis_ind = 1 --based on ED CAP
    group by
        diagnosis_encounter_all.visit_key,
        diagnosis_encounter_all.pat_key
)

select
    visit_key,
    pat_key,
    cohort_ind,
    ci_ind,
    complicated_pneumonia_ind,
    other_dx_ind
from
    cap_diagnoses
where
    --patient has ICD-10 code for Community-Acquired Pneumonia
    cohort_ind = 1
    --OR patient has ICD-10 code for Community-Acquired Pneumonia
    --Used for revisit/readmission calculation
    or complicated_pneumonia_ind = 1
