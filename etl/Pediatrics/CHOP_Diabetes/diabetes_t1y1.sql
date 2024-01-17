with sde_visit_keys as (
    select
		smart_data_element_all.visit_key,
        smart_data_element_all.element_value
    from
        {{ ref('smart_data_element_all') }} as smart_data_element_all
    where
        smart_data_element_all.concept_id = 'CHOP#7454' --CHOP DIABETES ENDO DISPOSITION
        and smart_data_element_all.element_value in ('T1Y1', 'T1Y1 modified (coaching in satellites)')
    group by
        smart_data_element_all.visit_key,
        smart_data_element_all.element_value
),

ip_t1y1 as (
	select
		encounter_inpatient.patient_key,
		encounter_inpatient.mrn,
		encounter_inpatient.encounter_date as ip_t1y1_dt,
		encounter_inpatient.hospital_admit_date,
		encounter_inpatient.hospital_discharge_date,
		encounter_inpatient.primary_dx,
		sde_visit_keys.element_value,
		row_number() over (
			partition by
				encounter_inpatient.patient_key
			order by
				encounter_inpatient.encounter_date
		) as ip_rn
	from
		{{ ref('encounter_inpatient') }} as encounter_inpatient
		inner join sde_visit_keys
			on sde_visit_keys.visit_key = encounter_inpatient.visit_key
	where
		encounter_inpatient.encounter_date <= current_date
),

specialty_care_sde_visit_keys as (
	select
		smart_data_element_all.visit_key
	from
		{{ ref('smart_data_element_all') }} as smart_data_element_all
	where
		smart_data_element_all.concept_id = 'CHOP#6984'	--Type of Diabetes
		and smart_data_element_all.element_value is not null
	group by
		smart_data_element_all.visit_key
),

op_t1y1 as (
	select
		encounter_specialty_care.patient_key,
		encounter_specialty_care.mrn,
		encounter_specialty_care.visit_type,
		encounter_specialty_care.encounter_date as op_t1y1_dt,
		row_number() over (
			partition by
				encounter_specialty_care.patient_key
			order by
				encounter_specialty_care.encounter_date
		) as op_rn
	from
		{{ ref('encounter_specialty_care') }} as encounter_specialty_care
		inner join specialty_care_sde_visit_keys
			on specialty_care_sde_visit_keys.visit_key = encounter_specialty_care.visit_key
	where
		encounter_specialty_care.visit_type in (
			'DIABETES T1Y1 NEW',
			'NEW DIABETES TYPE 1 TRANSFER',
			'NEW DIABETES TYPE 2 TRANSFER',
			'NEW DIABETES PATIENT',
			'NEW POSSIBLE DIABETES'
		)
		and encounter_specialty_care.encounter_date <= current_date
)
--Combine the two logics above and determine new diabetes cohort at CHOP: 
--including diagnosed at CHOP and transfered to CHOP

select
	stg_patient.patient_key,
	stg_patient.mrn,
	stg_patient.patient_name,
	--the earliest date (ip enc date) AT CHOP as Newly diagnosed diabetes patients
	coalesce(ip_t1y1.ip_t1y1_dt, op_t1y1.op_t1y1_dt) as new_diabetes_date,
	--new onset diabetes patients who admitted and diagnosed AT CHOP
	case
		when ip_t1y1.ip_t1y1_dt is not null
		then 1 else 0
	end as ip_diagnosis_ind,
	ip_t1y1.hospital_admit_date,
	ip_t1y1.hospital_discharge_date,
	ip_t1y1.primary_dx as ip_primary_dx,
	--diabetes patients newly transferred TO CHOP
	case
		when ip_t1y1.ip_t1y1_dt is null
		then 1 else 0
	end as new_transfer_ind,
	op_t1y1.visit_type as op_visit_type,
	op_t1y1.op_t1y1_dt as op_visit_date
from
	{{ ref('stg_patient') }} as stg_patient
	left join ip_t1y1
		on ip_t1y1.patient_key = stg_patient.patient_key
			and ip_t1y1.ip_rn = 1
	left join op_t1y1
		on op_t1y1.patient_key = stg_patient.patient_key
			--select the earliest OP visit as the new transfer TO CHOP
			and op_t1y1.op_rn = 1
	left join {{ ref('diabetes_t1y1_historical') }} as diabetes_t1y1_historical
		on stg_patient.patient_key = diabetes_t1y1_historical.patient_key
where
	coalesce(ip_t1y1.patient_key, op_t1y1.patient_key) is not null
	and diabetes_t1y1_historical.patient_key is null
union all
--	Old ICR Flowsheets has launched since 2012 to identify inpatients:
select
	diabetes_t1y1_historical.patient_key,
	diabetes_t1y1_historical.mrn,
	diabetes_t1y1_historical.patient_name,
	diabetes_t1y1_historical.new_diabetes_dt as new_diabetes_date,
	diabetes_t1y1_historical.ip_diag_ind as ip_diagnosis_ind,
    diabetes_t1y1_historical.hospital_admit_date,
    diabetes_t1y1_historical.hospital_discharge_date,
	diabetes_t1y1_historical.ip_primary_dx,
	diabetes_t1y1_historical.new_transfer_ind,
	diabetes_t1y1_historical.op_visit_type,
	diabetes_t1y1_historical.op_visit_date
from
	{{ ref('diabetes_t1y1_historical') }} as diabetes_t1y1_historical
