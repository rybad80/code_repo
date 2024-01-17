-- procedure and surgeries
with cohort_earliest_ov as (
	select
		mrn,
		pat_key,
		min(encounter_date) as earliest_ov_date
	from {{ref('stg_frontier_airway_enc_ov')}}
	group by
		mrn,
		pat_key
),
proc_or_case_lvl as (
	select proc_cpt.mrn,
		proc_cpt.pat_key,
		cohort_earliest_ov.earliest_ov_date,
		proc_cpt.visit_key,
		--proc_cpt.surgery_csn,
		proc_cpt.service_date,
		max(proc_cpt.airway_ent_proc_ind) as airway_ent_proc_ind,
		max(proc_cpt.bronchoscopy_ind) as bronchoscopy_ind,
		max(proc_cpt.microlaryngoscopy_ind) as microlaryngoscopy_ind,
		case when max(proc_cpt.bronchoscopy_ind) = 1 and max(proc_cpt.microlaryngoscopy_ind) = 1 then 1
			when max(proc_cpt.mlb_ind) = 1 then 1
			else 0 end as mlb_ind_final,
		max(proc_cpt.tracheostomy_ind) as tracheostomy_ind
	from {{ref('stg_frontier_airway_enc_proc_cpt')}} as proc_cpt
	left join cohort_earliest_ov as cohort_earliest_ov
		on proc_cpt.pat_key = cohort_earliest_ov.pat_key
	group by
        proc_cpt.mrn,
        proc_cpt.pat_key,
        cohort_earliest_ov.earliest_ov_date,
        proc_cpt.visit_key,
		--proc_cpt.surgery_csn,
        proc_cpt.service_date
    having
	--For MLB and Tracheostomy procedures, procedures could be performed prior to any office visits.
		(cohort_earliest_ov.earliest_ov_date is not null
			and proc_cpt.service_date >= cohort_earliest_ov.earliest_ov_date)
		or (mlb_ind_final = 1 or max(proc_cpt.tracheostomy_ind) = 1)
)

select
	stg_encounter.pat_key,
	stg_encounter.mrn,
	stg_encounter.visit_key,
	stg_encounter.encounter_date,
	max(proc_or_case_lvl.airway_ent_proc_ind) as airway_ent_proc_ind
from proc_or_case_lvl
inner join {{ref('stg_encounter')}} as stg_encounter
	on proc_or_case_lvl.visit_key = stg_encounter.visit_key
group by stg_encounter.pat_key,
	stg_encounter.mrn,
	stg_encounter.visit_key,
	stg_encounter.encounter_date
