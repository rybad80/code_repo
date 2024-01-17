select
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	diabetes_patient_all.last_visit_date,
	max(case
        when diabetes_patient_all.diabetes_reporting_month > encounter_specialty_care.encounter_date
        then encounter_specialty_care.encounter_date
    end) as most_recent_gi_date,
    max(case
        when diabetes_patient_all.diabetes_reporting_month - interval('15 month')
            <= date(encounter_specialty_care.encounter_date)
            and diabetes_patient_all.diabetes_reporting_month > date(encounter_specialty_care.encounter_date)
        then 1 else 0
    end) as last_15mo_gi_visit_ind,
    max(case
        when diabetes_patient_all.diabetes_reporting_month - interval('12 month')
            <= date(encounter_specialty_care.encounter_date)
            and diabetes_patient_all.diabetes_reporting_month > date(encounter_specialty_care.encounter_date)
        then 1 else 0
    end) as last_12mo_gi_visit_ind
from
    {{ ref('diabetes_patient_all') }} as diabetes_patient_all
    inner join {{ ref('encounter_specialty_care') }} as encounter_specialty_care
        on encounter_specialty_care.patient_key = diabetes_patient_all.patient_key
where
	lower(encounter_specialty_care.specialty_name) in ('gi/nutrition', 'clinical nutrition')
group by
    diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	diabetes_patient_all.last_visit_date
