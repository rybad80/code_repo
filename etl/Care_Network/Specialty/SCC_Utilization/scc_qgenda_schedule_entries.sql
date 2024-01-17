with department_base as (
	select
		department_care_network.department_name,
        department_care_network.dept_key,
		department_care_network.department_id,
		case
			when department_care_network.department_id = '101012175' then 'ONCOLOGY'					-- BWV HEMATOLOGY
			when department_care_network.department_id = '101033100' then 'GLOBAL PATIENT SERVICES'		-- BGR GLOBAL PAT SVCS
			when department_care_network.department_id = '89394025' then 'NEUROFIBROMATOSIS'			-- BGR NEUROFIBROMATOSIS
			when department_care_network.department_id = '89120003' then 'ANESTHESIA PAIN MANAGEMENT'	-- BGR ANES PAIN MGMT
			when department_care_network.department_id = '101001051' then 'GENERAL ANESTHESIA'			-- BGR ANES RESOURCE CTR
			when department_care_network.department_id = '101001405' then 'RESEARCH'					-- CSH CHPS OUTPATIENT
			else department_care_network.specialty_name
		end as specialty,
		case
			when department_care_network.scc_abbreviation = 'ABN' then 'ABG'
			when department_care_network.scc_abbreviation = 'VIRTUA' then 'VIR'
			when department_care_network.department_name like '%BGR%' then 'BGR'
			when department_care_network.department_center like 'BUERGER%' then 'BGR'
			when department_care_network.department_name like 'CSH CHPS OUTPATIENT%' then 'BGR'
			else department_care_network.scc_abbreviation end as site_abbreviation,
		department_care_network.department_center,
		department_id_rollup
	from {{ ref('department_care_network') }} as department_care_network
	left join {{ ref('lookup_care_network_bgr_department_floors') }} as lookup_care_network_bgr_department_floors
		on lookup_care_network_bgr_department_floors.department_id = department_care_network.department_id
	where site_abbreviation is not null
	and lower(specialty) != 'urgent care'
	-- remove departments that should not appear in downstream reporting
	and department_care_network.department_id not in (
		'80244010',				-- EXT DIABETES
		'80368021',				-- EXT NEONATOLOGY
		'81227005',				-- PNJ SANCHEZ CARDIOLOGY
		'81249010',				-- PNJ DIABETES
		'81376022',				-- PNJ NEPHROLOGY
		'82105001',				-- VNJ ADOLESCENT
		'82106001',				-- VNJ ANCILLARIES
		'82230005',				-- VNJ SANCHEZ CARDIOLOGY
		'82252010',				-- VNJ DIABETES
		'82389023',				-- VNJ NEUROLOGY
		'82706023',				-- VNJ PREP
		'83234523',				-- BUC - PREP
		'83240010',				-- BUC DIABETES
		'83371022',				-- BUC NEPHROLOGY
		'84408027',				-- KOP ORTHOPEDICS
		'89266502',				-- RCS-RESRCH VNJ ALLERGY
		'89267502',				-- RCS-RESRCH PNJ ALLERGY
		'101001076',			-- KOP SLEEP CENTER
		'101001083',			-- KOP ONCOLOGY HOLDING
		'101001608',			-- BGR DIALYSIS
		'101003033',			-- KOPH SLEEP CENTER
		'101012028',			-- KOP ADOPTION
		'101012111',			-- PB LGH GASTROENTRLGY
		'101012118',			-- KOP CEREBRAL PALSY
		'101013049',			-- KOP SMC CONCUSSION
		'101022016',			-- VIRTUA SLEEP LAB
		'101022064',			-- VIRTUA CAMDEN CARDIO
		'101012308',			-- KOP ORAL IMMUNOTHERAPY
		'1022008',				-- PB PRINCETON GEN PEDS
		'101029003'				-- IH MASSAGE THERAPY
	)
),

legacy_sites as (
	select
		department_name,
		dept_key,
		department_id,
		specialty,
		case
			when site_abbreviation = 'LGH' then 'LGH-Leg'
			when site_abbreviation = 'ABG' then 'ABG-Leg'
		end as site_abbreviation,
		department_center,
		department_id_rollup
	from department_base
	where site_abbreviation in (
		'ABG',
		'LGH'
	)
),

department_union as (
	select
		department_name,
		dept_key,
		department_id,
		specialty,
		site_abbreviation,
		department_center,
		department_id_rollup
	from department_base
	union all
	select
		department_name,
		dept_key,
		department_id,
		specialty,
		site_abbreviation,
		department_center,
		department_id_rollup
	from legacy_sites
),

behav_health_base as ( --region create base list of SCC Behavioral Health departments
	select
		department_union.department_id,
		department_union.department_name,
		department_union.site_abbreviation
	from department_union
	where lower(department_union.department_name) like '%behavioral health%'
),

qgenda_provider_departments as (
	select
		staffkey,
		site,
		qgenda_division_name,
		department_id,
		department_name,
		specialty_name
	from {{ ref('stg_qgenda_scc_division_tags') }}
	group by
		staffkey,
		site,
		qgenda_division_name,
		department_id,
		department_name,
		specialty_name
),

staffkey_department_rollup_transforms as (
	select distinct
		stg_qgenda_scc_provider.staffkey,
		stg_qgenda_scc_provider.staff_abbrev,
		stg_qgenda_scc_provider.provider_id,
		stg_qgenda_scc_provider.provider_key,
		stg_qgenda_scc_provider.provider_name,
		stg_qgenda_scc_provider.provider_type,
		stg_qgenda_scc_provider.location_name,
		stg_qgenda_scc_provider.site,
		stg_qgenda_scc_provider.specialty,
		behav_health_base.department_name,
		behav_health_base.department_id,
		case
			when site = 'PNJ' then '101016077'
			when stg_qgenda_scc_provider.staff_abbrev in (
				'[BH Pain-BGR12]'
			) then lookup_qgenda_bgr_staffabbrev.department_id
			else behav_health_base.department_id
		end as dep_id_rollup,
		coalesce(dep_id_rollup, behav_health_base.department_id) as provider_department_id
	from {{ ref('stg_qgenda_scc_provider') }} as stg_qgenda_scc_provider
	left join behav_health_base
        on behav_health_base.site_abbreviation = stg_qgenda_scc_provider.site
	left join {{ ref('lookup_qgenda_bgr_staffabbrev') }} as lookup_qgenda_bgr_staffabbrev
		on lookup_qgenda_bgr_staffabbrev.qgenda_staffabbrev = stg_qgenda_scc_provider.staff_abbrev
	where lower(stg_qgenda_scc_provider.specialty) = 'behavioral health services'
	union all
	select
		stg_qgenda_scc_provider.staffkey,
		stg_qgenda_scc_provider.staff_abbrev,
		stg_qgenda_scc_provider.provider_id,
		stg_qgenda_scc_provider.provider_key,
		stg_qgenda_scc_provider.provider_name,
		stg_qgenda_scc_provider.provider_type,
		stg_qgenda_scc_provider.location_name,
		stg_qgenda_scc_provider.site,
		stg_qgenda_scc_provider.specialty,
		department_union.department_name,
		department_union.department_id,
		case
			when stg_qgenda_scc_provider.site = 'BGR'
			and stg_qgenda_scc_provider.staff_abbrev like '[%'
				then lookup_qgenda_bgr_staffabbrev.department_id
			when stg_qgenda_scc_provider.specialty = 'GENETICS'
			and stg_qgenda_scc_provider.location_name = 'BGR9'
				then '101012166'			-- BGR GENETICS
			when stg_qgenda_scc_provider.specialty = 'GENETICS'
			and stg_qgenda_scc_provider.location_name = 'BGR12'
				then '101012141'			-- BGR MITOCHONDRIAL MED
			when stg_qgenda_scc_provider.staff_abbrev = 'PhillipsM'
			and stg_qgenda_scc_provider.site in ('LGH', 'LGH-Leg')
				then '101012103'			-- LGH GASTROENTEROLOGY
			when stg_qgenda_scc_provider.staff_abbrev = 'HachenR'
				then '84709012'				-- KOP NEUROFIBRAMOTOSIS
			when stg_qgenda_scc_provider.staff_abbrev in (
				'SilverD',
				'MagnussM'
			) and stg_qgenda_scc_provider.site = 'KOP'
				then '84324012'				-- KOP DIAG CENTER
			when stg_qgenda_scc_provider.staff_abbrev = 'ScribanP'
				and stg_qgenda_scc_provider.site = 'KOP'
				then '101012060'			-- KOP CARE CLINIC
			else department_id_rollup
		end as dep_id_rollup,
		coalesce(dep_id_rollup, department_union.department_id) as provider_department_id
	from {{ ref('stg_qgenda_scc_provider') }} as stg_qgenda_scc_provider
	left join department_union
		on department_union.site_abbreviation = stg_qgenda_scc_provider.site
		and department_union.specialty = stg_qgenda_scc_provider.specialty
	left join {{ ref('lookup_qgenda_bgr_staffabbrev') }} as lookup_qgenda_bgr_staffabbrev
		on lookup_qgenda_bgr_staffabbrev.qgenda_staffabbrev = stg_qgenda_scc_provider.staff_abbrev
	where lower(stg_qgenda_scc_provider.specialty) != 'behavioral health services'
),

staffkey_provider_department_rollup as (
	select
		staffkey,
		staff_abbrev,
		provider_id,
		provider_key,
		provider_name,
		provider_type,
		location_name,
		site,
		staffkey_department_rollup_transforms.specialty,
		provider_department_id,
		department_union.department_name as provider_department_name
	from staffkey_department_rollup_transforms
	left join department_union
		on department_union.department_id = staffkey_department_rollup_transforms.provider_department_id
	group by
		staffkey,
		staff_abbrev,
		provider_id,
		provider_key,
		provider_name,
		provider_type,
		location_name,
		site,
		staffkey_department_rollup_transforms.specialty,
		provider_department_id,
		provider_department_name
),

provider_linked_departments as ( --region
	select
		staffkey_provider_department_rollup.staffkey,
		staffkey_provider_department_rollup.provider_id,
		staffkey_provider_department_rollup.provider_name,
		staffkey_provider_department_rollup.staff_abbrev,
		staffkey_provider_department_rollup.provider_department_id,
		staffkey_provider_department_rollup.specialty,
		staffkey_provider_department_rollup.site
	from staffkey_provider_department_rollup
	inner join {{ source('cdw', 'provider_dept') }} as provider_dept
        on staffkey_provider_department_rollup.provider_key = provider_dept.prov_key
	left join qgenda_provider_departments
		on qgenda_provider_departments.staffkey = staffkey_provider_department_rollup.staffkey
		and qgenda_provider_departments.department_id = staffkey_provider_department_rollup.provider_department_id
	group by
		staffkey_provider_department_rollup.staffkey,
		staffkey_provider_department_rollup.provider_id,
		staffkey_provider_department_rollup.provider_name,
		staffkey_provider_department_rollup.staff_abbrev,
		staffkey_provider_department_rollup.provider_department_id,
		staffkey_provider_department_rollup.specialty,
		staffkey_provider_department_rollup.site
),

--region create table for each room, for each session for each date of all departments
--that could be matched to a provider or division encounter
schedule_entries_all_potential_departments as (
	select distinct
		schedule_entry.schedulekey,
		dim_date.fiscal_year,
		dim_date.fiscal_quarter,
		dim_date.month_int,
		cast(startdate as date) as resdate,
		dim_date.weekday_name,
		schedule_entry.starttime,
		staffkey_provider_department_rollup.staffkey,
		staffkey_provider_department_rollup.staff_abbrev,
		staffkey_provider_department_rollup.provider_id,
		staffkey_provider_department_rollup.provider_name,
		staffkey_provider_department_rollup.provider_type,
		stg_qgenda_scc_rooms.room_name,
		schedule_entry.taskkey,
		stg_qgenda_scc_rooms.session_type,
		stg_qgenda_scc_rooms.room_type,
		stg_qgenda_scc_rooms.exam_room_ind,
		staffkey_provider_department_rollup.provider_department_name as department_name,
		staffkey_provider_department_rollup.provider_department_id as department_id,
		stg_qgenda_scc_rooms.location_name,
		staffkey_provider_department_rollup.site,
		staffkey_provider_department_rollup.specialty,
		dim_date.business_day_ind
	from {{ ref('stg_qgenda_scc_schedule_entry_base') }} as schedule_entry
	inner join {{ ref('dim_date') }} as dim_date
        on dim_date.full_date = cast(startdate as date)
	inner join {{ ref('stg_qgenda_scc_location') }} as stg_qgenda_scc_location
        on schedule_entry.schedulekey = stg_qgenda_scc_location.schedulekey
	left join staffkey_provider_department_rollup
        on schedule_entry.staffkey = staffkey_provider_department_rollup.staffkey
		and stg_qgenda_scc_location.location_name = staffkey_provider_department_rollup.location_name
	left join {{ ref('stg_qgenda_scc_rooms') }} as stg_qgenda_scc_rooms
        on stg_qgenda_scc_rooms.taskkey = schedule_entry.taskkey
	left join provider_linked_departments
        on provider_linked_departments.staffkey = staffkey_provider_department_rollup.staffkey
		and provider_linked_departments.provider_department_id
			= staffkey_provider_department_rollup.provider_department_id
	-- exclude KOP Oncology after FY21 and KOPH Oncology during FY21
	where not(
		(staffkey_provider_department_rollup.provider_department_id = '101001016' and dim_date.fiscal_year > 2021)
		or (staffkey_provider_department_rollup.provider_department_id = '101003024' and dim_date.fiscal_year = 2021)
	)
	-- exclude ABG Legacy Urology after FY21 and ABG Urology during FY21
	and not (
		(staffkey_provider_department_rollup.provider_department_id = '101013018' and dim_date.fiscal_year > 2021)
		or (staffkey_provider_department_rollup.provider_department_id = '101013043' and dim_date.fiscal_year = 2021)
	)
	and not (
		(staffkey_provider_department_rollup.provider_department_id = '101003021' and dim_date.fiscal_year > 2021)
		or (staffkey_provider_department_rollup.provider_department_id = '101001143' and dim_date.fiscal_year = 2021)
	)
	-- exclude LGH Legacy after July 2023 and LGH before August 2023
	and not (
		(stg_qgenda_scc_rooms.site = 'LGH-Leg' and dim_date.full_date > '2023-07-31')
		or (stg_qgenda_scc_rooms.site = 'LGH' and dim_date.full_date < '2023-08-01')
	)
	and schedule_entry.schedulekey not in (
		select reservation_schedulekey from {{ ref('lookup_schedulekeys_to_remove') }}
	)
)

select * from schedule_entries_all_potential_departments
