with stg_bh_notes as (
select
	visit_key,
	signed_dt,
	version_author_provider_name as provider_name,
	final_author_prov_key as prov_key,
	version_author_service_name,
	block_last_update_date,
	max(signed_dt) over (partition by visit_key, version_author_service_name order by visit_key) as final_signed,
	max(
        block_last_update_date
	) over (partition by visit_key, version_author_service_name order by visit_key) as final_update,
	count(note_id) over (partition by visit_key order by visit_key) as number_notes
from {{ref('bh_notes')}}
where note_type in ('MH CONSULT NOTE', 'MH PROGRESS NOTE')
),

stg_bh_notes_dedup as (
select
	visit_key,
     provider_name,
	prov_key,
	number_notes,
	signed_dt,
	max(final_signed) over (partition by visit_key order by visit_key) as final_signed_max
from stg_bh_notes
where version_author_service_name != 'NOT APPLICABLE'
     and final_update = block_last_update_date
),
bh_notes as (
select distinct
	visit_key,
	provider_name,
	prov_key,
	number_notes,
	signed_dt
from stg_bh_notes_dedup
where
final_signed_max = signed_dt
),


los as (
select
	visit_key,
	sum(los_days) as los_days,
	sum(los_days_as_of_today) as los_days_as_of_today
	from {{ref('adt_bed')}}
group by visit_key
)

select
	encounter_all.visit_key,
	encounter_all.pat_key,
	encounter_all.patient_name,
	encounter_all.dob,
	encounter_all.age_years,
	encounter_all.mrn,
	encounter_all.sex,
	encounter_all.csn,
	encounter_all.encounter_date,
	bh_notes.provider_name as bh_provider_name,
	bh_notes.prov_key as bh_prov_key,
	bh_notes.number_notes as number_notes,
	bh_notes.signed_dt as bh_final_note_signed_dt,
	encounter_all.dept_key,
	encounter_all.department_name,
	encounter_all.hospital_admit_date,
	encounter_all.hospital_discharge_date,
	los.los_days,
	los.los_days_as_of_today,
	diagnosis_encounter_all.diagnosis_name as primary_diagnosis_name,
    diagnosis_encounter_all.icd10_code as primary_diagnosis_code,
    diagnosis_encounter_all.dx_key,
	encounter_all.ed_ind,
	encounter_all.inpatient_ind,
	case when encounter_all.hospital_discharge_date is null then 1 else 0 end as currently_admitted_ind,
	case when upper(encounter_all.department_center_abbr) like '%KOP%' then 1 else 0 end as koph_ind,
	case when upper(encounter_all.department_center_abbr) like 'PHL IP%' then 1 else 0 end as 	phl_main_ind
from bh_notes as bh_notes
inner join {{ref('encounter_all')}} as encounter_all
    on encounter_all.visit_key = bh_notes.visit_key
left join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
    on encounter_all.visit_key = diagnosis_encounter_all.visit_key
    and diagnosis_encounter_all.visit_primary_ind  = 1
left join los as los
	on encounter_all.visit_key = los.visit_key
where encounter_all.encounter_date >= '2018-01-01'
    and encounter_all.encounter_type = 'Hospital Encounter'
