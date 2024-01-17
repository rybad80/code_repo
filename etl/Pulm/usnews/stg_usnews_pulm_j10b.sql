select
    stg_usnews_pulm_j10a.submission_year,
    stg_usnews_pulm_j10a.start_date,
    stg_usnews_pulm_j10a.end_date,
    stg_usnews_pulm_j10a.pat_key,
    stg_usnews_pulm_j10a.visit_key,
    stg_usnews_pulm_j10a.mrn,
    stg_usnews_pulm_j10a.patient_name,
    stg_usnews_pulm_j10a.dob,
    stg_usnews_pulm_j10a.csn,
    stg_usnews_pulm_j10a.encounter_date,
    stg_usnews_pulm_j10a.icd10_code,
    stg_usnews_pulm_j10a.diagnosis_name,
    encounter_inpatient.visit_key as inpatient_visit_key,
    encounter_inpatient.encounter_date as inpatient_encounter_date,
    encounter_inpatient.hospital_discharge_date as inpatient_discharge_date,
    encounter_inpatient.hospital_discharge_date + interval '90 days' as inpatient_follow_up_date,
    encounter_inpatient.primary_dx_icd as inpatient_primary_icd10_code,
    encounter_inpatient.primary_dx as inpatient_diagnosis_name
from
    {{ref('stg_usnews_pulm_j10a')}} as stg_usnews_pulm_j10a
    inner join {{ref('encounter_inpatient')}} as encounter_inpatient
        on stg_usnews_pulm_j10a.pat_key = encounter_inpatient.pat_key
where
    encounter_inpatient.encounter_date >= stg_usnews_pulm_j10a.encounter_date
    and encounter_inpatient.encounter_date <= stg_usnews_pulm_j10a.end_date
    and lower(encounter_inpatient.primary_dx_icd) like 'j45.%'
    and lower(encounter_inpatient.hsp_acct_patient_class) = 'inpatient'
    