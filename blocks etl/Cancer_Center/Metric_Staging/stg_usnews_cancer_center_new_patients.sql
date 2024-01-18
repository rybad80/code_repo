{{ config(meta = {
    'critical': false
}) }}

select
	usnews_metadata_calendar.submission_year as submission_year,
	usnews_metadata_calendar.division,
	usnews_metadata_calendar.question_number,
	usnews_metadata_calendar.metric_id,
	usnews_metadata_calendar.metric_name,
	stg_cancer_center_new_patients.mrn as mrn,
	stg_patient.patient_name,
	stg_patient.dob,
	stg_cancer_center_new_patients.visit_date as index_date,
	stg_cancer_center_new_patients.pat_key as num,
	null as denom,
	stg_cancer_center_new_patients.pat_key as primary_key,
	null as cpt_code,
	'clinical' as domain, --noqa: L029
	null as subdomain,
	stg_cancer_center_new_patients.visit_date as metric_date
from {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
	inner join {{ ref('stg_cancer_center_new_patients') }} as stg_cancer_center_new_patients
		on stg_cancer_center_new_patients.visit_date
			between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
				and age_years between usnews_metadata_calendar.age_gte and usnews_metadata_calendar.age_lt
	inner join {{ ref('stg_patient') }} as stg_patient
		on stg_patient.pat_key = stg_cancer_center_new_patients.pat_key
	inner join {{ref('stg_cancer_center_tumor_registry')}} as stg_cancer_center_tumor_registry
		on stg_cancer_center_new_patients.pat_key = stg_cancer_center_tumor_registry.pat_key
where
	usnews_metadata_calendar.question_number in ('b6a', 'b6b')
	and histology_behavior_ind = 1
group by
usnews_metadata_calendar.submission_year,
usnews_metadata_calendar.division,
usnews_metadata_calendar.question_number,
usnews_metadata_calendar.metric_id,
usnews_metadata_calendar.metric_name,
stg_cancer_center_new_patients.mrn,
stg_patient.patient_name,
stg_patient.dob,
stg_cancer_center_new_patients.visit_date,
stg_cancer_center_new_patients.pat_key,
denom,
primary_key,
cpt_code,
domain,
subdomain
