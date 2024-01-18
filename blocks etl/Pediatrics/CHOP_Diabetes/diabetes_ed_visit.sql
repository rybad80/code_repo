select distinct
    diabetes_patient_all.diabetes_reporting_month,
    encounter_ed.encounter_key,
    diabetes_patient_all.patient_key,
    diabetes_patient_all.mrn,
    diabetes_patient_all.diabetes_type,
    diabetes_patient_all.first_dx_date,
    encounter_ed.encounter_date,
    encounter_ed.ed_arrival_date,
    encounter_ed.ed_discharge_date,
    --patient's chief complaint when visiting the ED (name corresponds to code)
    encounter_ed.primary_reason_for_visit_name,
    --all diagnosis names entered by the clinicians during the encounter
	encounter_ed.clinical_dx_all_dx_nm,
	encounter_ed.inpatient_ind,
	encounter_ed.revisit_72_hour_ind,
    case
        when lower(encounter_ed.primary_reason_for_visit_name) like '%diabetes%'
        then 1 else 0
    end as diabetes_for_visit_ind --flag daibetes as the primary reason for visit 
from
    {{ ref('diabetes_patient_all') }} as diabetes_patient_all
    inner join {{ ref('encounter_ed') }} as encounter_ed
        on encounter_ed.patient_key = diabetes_patient_all.patient_key
            and encounter_ed.encounter_date < diabetes_patient_all.diabetes_reporting_month
            and encounter_ed.encounter_date >= diabetes_patient_all.diabetes_reporting_month - interval('15 month')
where
	date(encounter_ed.ed_arrival_date) > diabetes_patient_all.first_dx_date
