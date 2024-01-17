select distinct
    diabetes_patient_all.diabetes_reporting_month,
    encounter_inpatient.encounter_key,
    diabetes_patient_all.patient_key,
    diabetes_patient_all.mrn,
    diabetes_patient_all.diabetes_type,
    diabetes_patient_all.first_dx_date,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.admission_service,
    encounter_inpatient.hospital_discharge_date,
    encounter_inpatient.discharge_service,
    encounter_inpatient.discharge_provider_name,
    encounter_inpatient.primary_dx,
    encounter_inpatient.ed_ind,
    encounter_inpatient.icu_ind,
    case
        when lower(encounter_inpatient.primary_dx) like '%diabetes%'
        then 1 else 0
    end as diabetes_for_visit_ind --flag daibetes as the primary reason for visit
from
    {{ ref('diabetes_patient_all') }} as diabetes_patient_all
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on encounter_inpatient.patient_key = diabetes_patient_all.patient_key
            and encounter_inpatient.encounter_date < diabetes_patient_all.diabetes_reporting_month
            and encounter_inpatient.encounter_date
            >= diabetes_patient_all.diabetes_reporting_month - interval('15 month')
where
    date(encounter_inpatient.hospital_admit_date) > diabetes_patient_all.first_dx_date
