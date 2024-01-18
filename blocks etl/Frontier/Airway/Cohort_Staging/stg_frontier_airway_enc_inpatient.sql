-- procedure and surgeries
with inpat_all as (
	select c.*,
		i.admission_service,
		i.discharge_service,
		stg_encounter.hospital_admit_date,
		extract( --noqa: PRS
			epoch from stg_encounter.hospital_discharge_date - stg_encounter.hospital_admit_date
		) / 86400.0 as hospital_los_days
	from {{ref('stg_frontier_airway_enc_procedure')}} as p
	inner join {{ref('stg_encounter_inpatient')}} as i
		on p.visit_key = i.visit_key
	inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = i.visit_key
	inner join {{ref('stg_frontier_airway_enc_proc_cpt')}} as c
		on p.visit_key = c.visit_key
	where stg_encounter.encounter_date between '2017-07-01' and current_date
),
transfer_pt as (
	select
		note_edit_metadata_history.visit_key,
		note_edit_metadata_history.encounter_date,
		note_edit_metadata_history.mrn
	from {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
	inner join inpat_all as ips
		on ips.visit_key = note_edit_metadata_history.visit_key
	inner join {{source('cdw', 'note_text')}} as note_text
		on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
	where
		last_edit_ind = 1
		and ((note_type_id = 5 --'DISCHARGE SUMMARY'
				and lower(note_text) like '%transferred to chop for% laryngeal web % subglottic stenosis %'
			) or (note_type_id = 400002 --'TRANSFER NOTE'
				and lower(note_text) like '%transferred to chop for an airway evaluation%'
			)
		)
	group by
		note_edit_metadata_history.visit_key,
		note_edit_metadata_history.encounter_date,
		note_edit_metadata_history.mrn
),
admit_dx_n_service_pt as (
	select
		ips.visit_key,
		ips.mrn,
		max(case when ips.admission_service in ('Otolaryngology', 'Otorhinolaryngology')
				and ips.discharge_service in ('Otolaryngology', 'Otorhinolaryngology')
				then 1 else 0 end) as admit_disch_sv_ent_ind,
		max(case when lkdx.lookup_dx_id is not null and dea.icd10_code != 'D18.1' --Lymphangioma
				then 1 else 0 end) as admit_prim_dx_airway_ind
	from inpat_all as ips
	left join {{ref('diagnosis_encounter_all')}} as dea
		on ips.visit_key = dea.visit_key
		and (dea.ip_admit_primary_ind = 1 or dea.hsp_acct_admit_primary_ind = 1)
	left join {{ref('lookup_frontier_program_diagnoses')}} as lkdx
		on  dea.icd10_code = cast(lkdx.lookup_dx_id as nvarchar(20))
		and lkdx.program = 'airway'
	group by
		ips.visit_key,
		ips.mrn
),
or_case as (
	select sp.mrn,
		sp.visit_key,
		sp.surgery_csn,
		coalesce(max(ips.hospital_los_days), seconds_between(max(ips.hospital_admit_date), current_timestamp) / 86400.0)
			as hospital_los_days,
		max(case when a.cpt_code is not null then 1 else 0 end) as airway_proc_ind,
		max(coalesce(a.airway_ent_proc_ind, 0)) as airway_ent_proc_ind,
		max(coalesce(a.mlb_ind, 0)) as mlb_ind,
		max(coalesce(a.tracheostomy_ind, 0)) as tracheostomy_ind,
		max(coalesce(a.complex_airway_proc_ind, 0)) as complex_airway_proc_ind,
		max(case when sp.service not in ('Otolaryngology', 'Pulmonary', 'Gastroenterology') then 1 else 0 end)
			as service_other_ind
	from inpat_all as ips
	inner join {{ref('surgery_procedure')}} as sp
		on ips.visit_key = sp.visit_key
	left join {{ref('stg_frontier_airway_enc_proc_cpt')}} as a
		on a.visit_key = sp.visit_key
		and a.cpt_code = sp.cpt_code
		and a.surgery_csn = sp.surgery_csn
	group by
		sp.mrn,
		sp.visit_key,
		sp.surgery_csn
),
or_airway_proc_rate as (
	select or_case.mrn,
		or_case.visit_key,
		max(or_case.hospital_los_days) as hospital_los_days,
		count(surgery_csn) as or_case_count,
		-- sum(airway_proc_ind) as or_case_w_airway_count,
		-- or_case_w_airway_count * 1.0 / or_case_count as airway_proc_rate,
		sum(airway_ent_proc_ind) as or_case_w_airway_ent_count,
		or_case_w_airway_ent_count * 1.0 / or_case_count as airway_ent_proc_rate,
		sum(complex_airway_proc_ind) as or_case_w_airway_cmplx_count,
		sum(service_other_ind) as or_case_w_other_service_count,
		max(case when transfer_pt.visit_key is not null then 1 else 0 end) as transfer_for_airway_ind,
		max(admit_disch_sv_ent_ind) as admit_disch_sv_ent_ind,
		max(admit_prim_dx_airway_ind) as admit_prim_dx_airway_ind
	from or_case
	left join admit_dx_n_service_pt
		on or_case.visit_key = admit_dx_n_service_pt.visit_key
	left join transfer_pt
		on or_case.visit_key = transfer_pt.visit_key
	group by
		or_case.mrn,
		or_case.visit_key
)
--Airway specific Inpatient encounters
select
	mrn,
	visit_key,
	1 as airway_ip_ind_final
from or_airway_proc_rate
where transfer_for_airway_ind = 1
	or (admit_disch_sv_ent_ind = 1 and airway_ent_proc_rate = 1 and admit_prim_dx_airway_ind = 1)
	or (hospital_los_days < 30
		and airway_ent_proc_rate = 1
		and (or_case_w_other_service_count = 0 or or_case_w_airway_cmplx_count = 1))
	or (hospital_los_days >= 30
		and airway_ent_proc_rate = 1
		and (or_case_w_other_service_count = 0 or or_case_w_airway_cmplx_count = 1)
		and admit_prim_dx_airway_ind = 1)
