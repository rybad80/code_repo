{{ config(meta = {
    'critical': true
}) }}

select
    stg_outbreak_covid_cohort_patient_encounter.visit_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_outbreak_covid_cohort_patient_encounter.covid_start_date,
    stg_outbreak_covid_cohort_patient_encounter.covid_end_date,
    stg_outbreak_covid_cohort_patient_encounter.source_summary,
    stg_outbreak_covid_cohort_patient_encounter.covid_active_ind,
    stg_outbreak_covid_cohort_patient_encounter.positive_covid_test_ind,
    stg_outbreak_covid_cohort_patient_encounter.covid_diagnosis_ind,
    stg_outbreak_covid_cohort_patient_encounter.covid_bugsy_ind,
    stg_outbreak_covid_cohort_patient_encounter.pstu_siu_team_ind,
    stg_outbreak_covid_cohort_patient_encounter.pstu_ind,
    stg_outbreak_covid_cohort_patient_encounter.siu_ind,
    stg_outbreak_covid_cohort_patient_encounter.inpatient_ind,
    stg_outbreak_covid_cohort_patient_encounter.admission_department_center_abbr,
    stg_outbreak_covid_cohort_patient_encounter.pat_key
from
    {{ref('stg_outbreak_covid_cohort_patient_encounter')}}
        as stg_outbreak_covid_cohort_patient_encounter
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key
        = stg_outbreak_covid_cohort_patient_encounter.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key
        = stg_outbreak_covid_cohort_patient_encounter.pat_key
