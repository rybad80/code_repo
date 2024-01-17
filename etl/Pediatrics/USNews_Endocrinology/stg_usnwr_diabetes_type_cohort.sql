with outpatient_first_visit as (
	select
		encounter_specialty_care.patient_key,
		encounter_specialty_care.mrn,
		encounter_specialty_care.visit_type,
		encounter_specialty_care.encounter_date as op_first_date,
		row_number() over (
			partition by
				encounter_specialty_care.patient_key
			order by
				encounter_specialty_care.encounter_date
		) as op_rn
	from
        {{ ref('encounter_specialty_care') }} as encounter_specialty_care
	where
		encounter_specialty_care.visit_type in (
            'NEW DIABETES TYPE 1 TRANSFER',
            'NEW DIABETES PATIENT',
            'NEW DIABETES TYPE 2 TRANSFER',
            'FOLLOW UP DIABETES',
            'DIABETES T1Y1 FOLLOW UP',
            'VIDEO VISIT DIABETES'
		)
),

inpatient_dx_date as (
    select
        encounter_inpatient.patient_key,
        encounter_inpatient.mrn,
        encounter_inpatient.encounter_date as ip_date,
        encounter_inpatient.hospital_admit_date,
        encounter_inpatient.hospital_discharge_date,
        encounter_inpatient.primary_dx,
        row_number() over (
            partition by
                encounter_inpatient.patient_key
            order by
                encounter_inpatient.encounter_date
        ) as ip_rn
from
	{{ ref('encounter_inpatient') }} as encounter_inpatient
where
    lower(encounter_inpatient.primary_dx) like '%diabetes%'
)

select distinct
    diabetes_patient_all.patient_key,
    -- keeping this because smart_data_element doesn't have a patient_key
    diabetes_patient_all.pat_key,
    diabetes_patient_all.patient_name,
    diabetes_patient_all.mrn,
    diabetes_patient_all.dob,
    stg_usnwr_diabetes_calendar.encounter_key,
    stg_usnwr_diabetes_calendar.endo_visit_date,
    inpatient_dx_date.ip_date as new_ip_diabetes_date,
    outpatient_first_visit.op_first_date as first_outpatient_date,
    case when inpatient_dx_date.ip_date is not null then 1 else 0 end as ip_diagnosis_ind,
    case when inpatient_dx_date.ip_date is null then 1 else 0 end as new_transfer_ind,
    case
        when outpatient_first_visit.op_first_date between current_date - interval('1 year') and current_date
            and inpatient_dx_date.ip_date is null --new_transfer_ind = '1'
        then 1 else 0
    end as transfer_exclu_ind,
    diabetes_patient_all.first_dx_date,
    diabetes_patient_all.payor_group,
    round(months_between(current_date, diabetes_patient_all.dob) / 12, 2) as current_age,
    case
        when diabetes_patient_all.diabetes_type is null then null
        when diabetes_patient_all.diabetes_type in (
            'Antibody negative Type 1',
            'Antibody positive Type 1',
            'Type 1 unknown antibody status'
        ) then 'Type 1'
        when diabetes_patient_all.diabetes_type = 'Type 2'
        then 'Type 2' else 'Other'
    end as diabetes_type_12,
    diabetes_patient_all.last_prov_type,
    coalesce(round(months_between(current_date, diabetes_patient_all.first_dx_date) / 12, 2),
        (year(current_date) - year(diabetes_patient_all.first_dx_date))) as duration_year,
    stg_usnwr_diabetes_calendar.diabetes_usnwr_year,
    stg_usnwr_diabetes_calendar.start_date,
    stg_usnwr_diabetes_calendar.end_date
from
    {{ref('diabetes_patient_all') }} as diabetes_patient_all
    inner join {{ref('stg_usnwr_diabetes_calendar') }} as stg_usnwr_diabetes_calendar
        on diabetes_patient_all.patient_key = stg_usnwr_diabetes_calendar.patient_key
            and stg_usnwr_diabetes_calendar.endo_visit_date between stg_usnwr_diabetes_calendar.start_date
                and stg_usnwr_diabetes_calendar.end_date
    left join inpatient_dx_date
        on inpatient_dx_date.patient_key = diabetes_patient_all.patient_key
            and inpatient_dx_date.ip_rn = '1'
    left join outpatient_first_visit
        on outpatient_first_visit.patient_key = diabetes_patient_all.patient_key
            and outpatient_first_visit.op_rn = '1'
where
    duration_year >= '1'
    and year(diabetes_patient_all.first_dx_date) < (stg_usnwr_diabetes_calendar.diabetes_usnwr_year - 1)
    and diabetes_patient_all.last_visit_type in (
        'NEW DIABETES TYPE 1 TRANSFER',
        'NEW DIABETES PATIENT',
        'NEW DIABETES TYPE 2 TRANSFER',
        'FOLLOW UP DIABETES',
        'DIABETES T1Y1 FOLLOW UP',
        'VIDEO VISIT DIABETES'
    )
