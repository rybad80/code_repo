--all inpatient encounters for Community-Acquired Pneumonia since FY21
select
    encounter_inpatient.visit_key,
    encounter_inpatient.pat_key,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.hospital_discharge_date,
    encounter_inpatient.inpatient_admit_date,
    encounter_inpatient.inpatient_los_days,
    encounter_inpatient.patient_name,
    encounter_inpatient.mrn,
    encounter_inpatient.csn,
    encounter_inpatient.dob,
    encounter_inpatient.sex,
    encounter_inpatient.admission_department_center_abbr as admission_campus,
    encounter_inpatient.admission_service,
    encounter_inpatient.discharge_service,
    --was IP LOS greater than 7 full days, rounded down?
    case when encounter_inpatient.inpatient_los_days >= 8
        then 1 else 0 end as los_7_days_ind,
    encounter_inpatient.icu_los_days,
    round(encounter_inpatient.age_years, 2) as age_years,
    stg_asp_ip_cap_cohort_all.ci_ind,
    stg_asp_ip_cap_cohort_all.complicated_pneumonia_ind,
    stg_asp_ip_cap_cohort_all.other_dx_ind
from
    {{ref('encounter_inpatient')}} as encounter_inpatient
    inner join {{ref('stg_asp_ip_cap_cohort_all')}} as stg_asp_ip_cap_cohort_all
        on encounter_inpatient.visit_key = stg_asp_ip_cap_cohort_all.visit_key
where
    encounter_inpatient.hospital_discharge_date >= '07/01/2020'
    and stg_asp_ip_cap_cohort_all.cohort_ind = 1
