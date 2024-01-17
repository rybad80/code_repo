with provider_staffkeys as ( --region create base list of all staffkeys found in RAM extracted data
	select
		staffkey
	from {{ source('qgenda_ods', 'staffmember') }}
	where compkey = '1121178a-aa59-4654-9160-043975c9fff1'
	group by
		staffkey
	union
	select
		staffkey
	from {{ ref('stg_qgenda_scc_schedule_entry_base') }}
	group by
		staffkey
),

--region apply missing abbreviations and IDs to QGenda data where it is missing or incorrect
stg_provider_ids as (
	select
		provider_staffkeys.staffkey,
		case
			when schedule_entry.staffabbrev = 'BettancL' then 'BetancoL'
			when schedule_entry.staffabbrev = 'Lizano' then 'LizanoR'
			when schedule_entry.staffabbrev = 'ElanaM' then 'MitchelE'
			when schedule_entry.staffabbrev = 'WinklesK' then 'DaTorreK'
			when schedule_entry.staffabbrev is null then staffmember.abbrev
			else schedule_entry.staffabbrev
		end as staff_abbrev,
		case
			when provider_staffkeys.staffkey = '04bb2ecd-82f1-4064-a138-c8d9560a0dcc' then '939609'
			when provider_staffkeys.staffkey = '8c23d605-b8bc-4b29-b2d4-43fd6979ff8b' then '970407'
			when schedule_entry.staffabbrev like '%[%' then null
			when staffmember.abbrev like '%[%' then null
			when staffmember.staffid is null then schedule_entry.staffid
			else staffmember.staffid
		end as provider_id
	from provider_staffkeys as provider_staffkeys
	left join {{ ref('stg_qgenda_scc_schedule_entry_base') }} as schedule_entry
        on schedule_entry.staffkey = provider_staffkeys.staffkey
	left join {{ source('qgenda_ods', 'staffmember') }} as staffmember
        on staffmember.staffkey = provider_staffkeys.staffkey
		and staffmember.compkey = '1121178a-aa59-4654-9160-043975c9fff1'
	group by
		provider_staffkeys.staffkey,
		staff_abbrev,
		provider_id
),

stg_provider_type as (
	select
		stg_provider_ids.staffkey,
		stg_provider_ids.staff_abbrev,
		stg_provider_ids.provider_id,
		coalesce(initcap(worker_provider.display_name), initcap(ext_provider.full_nm)) as provider_name,
		coalesce(initcap(ext_provider.prov_type), initcap(worker_provider.job_title)) as provider_type,
		coalesce(worker_provider.prov_key, ext_provider.prov_key) as provider_key
	from stg_provider_ids
	left join {{ source('cdw', 'provider') }} as ext_provider
		on ext_provider.ext_id = stg_provider_ids.provider_id
		and ext_provider.active_stat != 'Inactive'
	left join {{ ref('worker') }} as worker_provider
		on worker_provider.worker_id = stg_provider_ids.provider_id
),

stg_provider_location as (
	select
		staffkey,
		location_name,
		site
	from {{ ref('stg_qgenda_scc_location') }}
	group by
		staffkey,
		location_name,
		site
)

select distinct
	stg_provider_type.staffkey,
	stg_provider_type.staff_abbrev,
	stg_provider_type.provider_id,
	stg_provider_type.provider_name,
	stg_provider_type.provider_type,
	stg_provider_type.provider_key,
	location_name,
	site,
	case
		when stg_provider_type.staffkey = 'cda22947-835e-41e2-b013-d10257e53d57'
		and site = 'ABG'
			then 'ORTHOPEDICS'
		when stg_provider_type.staff_abbrev in (
			'Chang B',
			'MendenhallS'
		) and location_name = 'BGR11'
		then 'PLASTIC SURGERY'
		when stg_provider_type.staffkey in (
			'a1a0c716-23da-459f-aa8d-cec706d63aa8'
		) then 'ALLERGY'
		when stg_provider_type.staffkey = 'ea516a12-9e33-4a84-af40-f6ddf6301efb'
			then 'GENERAL PEDIATRICS'
		when stg_provider_type.staffkey in (
			'bd7e63a8-049a-482f-ba19-be8b2ef33944'
		) then 'DEVELOPMENTAL PEDIATRICS'
		when stg_provider_type.staffkey in (
			'5ef5f6d9-836f-45eb-84b2-9782ad3887e4'
		) then 'GASTROENTEROLOGY'
		when stg_provider_type.staff_abbrev = '[FoodChal-BGR1]'
			then 'ALLERGY'
		else stg_qgenda_scc_specialty.specialty
	end as specialty
from stg_provider_type
left join {{ ref('stg_qgenda_scc_specialty') }} as stg_qgenda_scc_specialty
	on stg_provider_type.staffkey = stg_qgenda_scc_specialty.staffkey
left join stg_provider_location
	on stg_provider_location.staffkey = stg_provider_type.staffkey
left join {{ ref('lookup_qgenda_bgr_staffabbrev') }} as lookup_qgenda_bgr_staffabbrev
	on lookup_qgenda_bgr_staffabbrev.qgenda_staffabbrev = stg_provider_type.staff_abbrev
