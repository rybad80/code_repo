with department_base as (
	select
		department_care_network.department_name,
        department_care_network.dept_key,
		department_care_network.department_id,
		department_care_network.specialty_name,
		case
			when scc_abbreviation = 'ABN' then 'ABG'
			when scc_abbreviation = 'VIRTUA' then 'VIR'
			when department_care_network.department_name like '%BGR%' then 'BGR'
			when department_center like 'BUERGER%' then 'BGR'
			when department_care_network.department_name like 'CSH CHPS OUTPATIENT%' then 'BGR'
			else scc_abbreviation
		end as site_abbreviation,
		lookup_care_network_bgr_department_floors.location_name,
		lookup_care_network_bgr_department_floors.department_id_rollup,
		department_center,
		specialty_care_ind,
		scc_ind
	from {{ ref('department_care_network') }} as department_care_network
	left join {{ ref('lookup_care_network_bgr_department_floors') }} as lookup_care_network_bgr_department_floors
		on lookup_care_network_bgr_department_floors.department_id = department_care_network.department_id
	where site_abbreviation is not null
),

behav_health_base as (
	select
		department_id,
		department_name,
		specialty_name,
		site_abbreviation
	from department_base
	where lower(department_name) like '%behavioral health%'
	and site_abbreviation not like 'BGR'
),

behav_health_rollup as (
	select
		department_base.department_id as department_base_id,
		department_base.department_name as department_base_name,
		department_base.location_name,
		case
			when department_base.site_abbreviation = 'PNJ' then '101016077'
			when department_base.site_abbreviation = 'EXT' then '80230543'
			when department_base.site_abbreviation = 'VIR' then '101016041'
			else behav_health_base.department_id
		end as department_id,
		case
			when department_base.site_abbreviation = 'PNJ' then 'PNJ BEHAVIORAL HEALTH'
			when department_base.site_abbreviation = 'EXT' then 'EXT BEHAVIORAL HEALTH'
			when department_base.site_abbreviation = 'VIR' then 'VIR BEHAVIORAL HEALTH'
			else behav_health_base.department_name
		end as department_name,
		department_base.site_abbreviation
	from department_base
	left join behav_health_base
		on department_base.site_abbreviation = behav_health_base.site_abbreviation
	where department_base.specialty_name = 'BEHAVIORAL HEALTH SERVICES'
),

stg_behav_health_encounters as (
	select
		visit_key,
		case
			when encounter_specialty_care.department_id in (
				'101016125',
				'92208529'
			) then encounter_specialty_care.department_id
			else behav_health_rollup.department_id
		end as department_id,
		case
			when encounter_specialty_care.department_id in (
				'101016125',
				'92208529'
			) then encounter_specialty_care.department_name
			else behav_health_rollup.department_name
		end as department_name,
		behav_health_rollup.site_abbreviation
	from {{ ref('encounter_specialty_care') }} as encounter_specialty_care
	left join behav_health_rollup
		on behav_health_rollup.department_base_id = encounter_specialty_care.department_id
	where lower(encounter_specialty_care.specialty_name) like '%behavioral health%'
	and site_abbreviation is not null
),

bgr_excluded_visits as (
	select
		visit_key
	from {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
	inner join {{ ref('lookup_care_network_bgr_excluded_visit_type_ids') }} as lookup_care_network_bgr_excluded_visit_type_ids --noqa: L016
		on lookup_care_network_bgr_excluded_visit_type_ids.visit_type_id = stg_encounter_outpatient.visit_type_id
	where department_center = 'BUERGER CENTER FOR ADVANCED PEDIATRIC MEDICINE'
),

stg_encounter as (
	select
		stg_encounter_outpatient.visit_key,
		department_base.department_name,
		department_base.department_id,
		case
			when stg_encounter_outpatient.department_id = 101022062 then 81117002	-- PNJ IMMUNOLOGY
			when stg_encounter_outpatient.department_id = 101022022 then 101022061	-- VIR IMMUNOLOGY
			when stg_encounter_outpatient.department_id = 101029003 then 101029001  -- IH MASSAGE THERAPY
			else coalesce(
				department_base.department_id_rollup,
				stg_behav_health_encounters.department_id,
				stg_encounter_outpatient.department_id
			)
		end as department_id_rollup,
		department_base.location_name,
		department_base.site_abbreviation,
		dim_date.fiscal_quarter,
		dim_date.fiscal_year,
		appointment_status
	from {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
	inner join {{ ref('dim_date') }} as dim_date
		on stg_encounter_outpatient.encounter_date = dim_date.full_date
	inner join department_base
		on department_base.dept_key = stg_encounter_outpatient.dept_key
	left join stg_behav_health_encounters
		on stg_encounter_outpatient.visit_key = stg_behav_health_encounters.visit_key
	where dim_date.full_date between '2020-07-01' and (current_date - 1)
	and dim_date.business_day_ind = 1
	and (lower(appointment_status) in ('completed', 'arrived')
		or encounter_type = 'hospital encounter')
	and stg_encounter_outpatient.specialty_care_ind = 1
	and stg_encounter_outpatient.visit_key not in (
		select visit_key from bgr_excluded_visits
	)
),

stg_specialty_care_encounter as (
	select
		encounter_specialty_care.visit_key,
		encounter_specialty_care.mrn,
		encounter_specialty_care.csn,
		encounter_specialty_care.encounter_date,
		stg_encounter.fiscal_year,
		stg_encounter.fiscal_quarter,
		encounter_specialty_care.provider_name,
		provider.prov_id as provider_id,
		provider.ext_id,
		-- change to provider.rpt_group_1 which is the worker_id and which QGenda is using as the new StaffId
		worker.worker_id,
		provider.prov_type,
		stg_encounter.department_name,
		stg_encounter.department_id_rollup as department_id,
		case
			when stg_encounter.department_name = 'CSH CHPS OUTPATIENT' then 'BGR12'
			else stg_encounter.location_name
		end as location_name,
		encounter_specialty_care.specialty_name,
		stg_encounter.site_abbreviation,
		stg_encounter.appointment_status,
		encounter_specialty_care.appointment_date,
		encounter_specialty_care.telehealth_ind,
		extract(hour from encounter_specialty_care.appointment_date) as hour_of_day,
		case
			when hour_of_day < 12 then 'AM'
			when hour_of_day between 12 and 17 then 'PM'
			else 'EVE'
		end as am_or_pm
	from {{ ref('encounter_specialty_care') }} as encounter_specialty_care
	inner join stg_encounter
		on stg_encounter.visit_key = encounter_specialty_care.visit_key
	left join {{ source('cdw', 'provider') }} as provider
        on provider.prov_key = encounter_specialty_care.prov_key
	left join {{ ref('worker') }} as worker
		on worker.prov_key = encounter_specialty_care.prov_key
	where (stg_encounter.site_abbreviation != 'BGR'
			and hour_of_day < 17)
	or stg_encounter.site_abbreviation = 'BGR'
)

select * from stg_specialty_care_encounter
