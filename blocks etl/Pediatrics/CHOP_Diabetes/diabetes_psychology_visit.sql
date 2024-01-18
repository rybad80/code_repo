with psychology_visit as ( --visit-level
select
	diabetes_patient_all.patient_key,
	encounter_specialty_care.visit_key as psychology_visit_key,
	encounter_specialty_care.encounter_date as psychology_dt,
	encounter_specialty_care.specialty_name,
	encounter_specialty_care.department_name,
	encounter_specialty_care.provider_name
from
	{{ref('diabetes_patient_all')}} as diabetes_patient_all
	inner join {{ref('encounter_specialty_care')}} as encounter_specialty_care
            on encounter_specialty_care.patient_key = diabetes_patient_all.patient_key
	inner join {{source('cdw', 'provider')}} as provider on provider.prov_key = encounter_specialty_care.prov_key
						and lower(prov_type) in ('psychologist')
group by
	diabetes_patient_all.patient_key,
	psychology_visit_key,
	psychology_dt,
	encounter_specialty_care.specialty_name,
	encounter_specialty_care.department_name,
	encounter_specialty_care.provider_name
)
select
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	psychology_visit.psychology_visit_key,
	psychology_visit.psychology_dt,
	psychology_visit.specialty_name,
	psychology_visit.department_name,
	psychology_visit.provider_name
from
	{{ref('diabetes_patient_all')}} as diabetes_patient_all
	inner join psychology_visit	on psychology_visit.patient_key = diabetes_patient_all.patient_key
						and psychology_visit.psychology_dt < diabetes_patient_all.diabetes_reporting_month
						and psychology_visit.psychology_dt >= diabetes_patient_all.diabetes_reporting_month - interval('15 month')
group by
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	psychology_visit.psychology_visit_key,
	psychology_visit.psychology_dt,
	psychology_visit.specialty_name,
	psychology_visit.department_name,
	psychology_visit.provider_name
