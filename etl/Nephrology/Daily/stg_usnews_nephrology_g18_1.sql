with ip_admits as (
	select
		cast(encounter_inpatient.visit_key as varchar(19)) as primary_key,
		encounter_inpatient.patient_name,
		encounter_inpatient.pat_key,
		encounter_inpatient.mrn,
		encounter_inpatient.visit_key,
		'hospital encounter' as encounter_type,
		encounter_inpatient.discharge_department as department_name,
		encounter_inpatient.encounter_date as index_date,
		1 as admit_ind
	from
		{{ref('encounter_inpatient')}} as encounter_inpatient
	where
		lower(encounter_inpatient.admission_service) like '%neph%'
),

ip_consults as (
	select
		{{dbt_utils.surrogate_key([
			'procedure_billing.mrn', 
			'procedure_billing.service_date', 
			'procedure_billing.cpt_code'])}} as primary_key,
		procedure_billing.patient_name,
		procedure_billing.pat_key,
		procedure_billing.mrn,
		procedure_billing.visit_key,
		procedure_billing.procedure_name as encounter_type,
		procedure_billing.department_name,
		procedure_billing.service_date as index_date,
		0 as admit_ind
	from
		{{ref('procedure_billing')}} as procedure_billing
	where
		procedure_billing.cpt_code in ('99255', '99254', '99253', '99251')
		and lower(procedure_billing.provider_specialty) in ('nep', 'kidney trans')
	group by
		primary_key,
		procedure_billing.patient_name,
		procedure_billing.pat_key,
		procedure_billing.mrn,
		procedure_billing.visit_key,
		procedure_billing.procedure_name,
		procedure_billing.department_name,
		index_date
),

all_ip_admits_consults as (
	select * from ip_admits
	union all
	select * from ip_consults
)

select
	usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.question_number,
	usnews_metadata_calendar.metric_id,
	usnews_metadata_calendar.division,
	all_ip_admits_consults.patient_name,
	all_ip_admits_consults.pat_key,
	all_ip_admits_consults.mrn, 
	stg_patient.dob,
	visit_key,
	index_date,
	encounter_type,
	department_name,
	admit_ind,
	primary_key
from
	all_ip_admits_consults
	inner join {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
	    on usnews_metadata_calendar.question_number = 'g18.1'
	    and index_date between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
	left join {{ ref('stg_patient') }} as stg_patient
	    on all_ip_admits_consults.pat_key = stg_patient.pat_key
