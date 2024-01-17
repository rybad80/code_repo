with specialty_lead as (
	select
		scheduleentry_stafftags.schedulekey,
		schedule_entry.staffkey,
		stafftags_tags_name,
		lead(stafftags_tags_name) over(
			partition by scheduleentry_stafftags.schedulekey order by upd_dt) as specialty_correction,
		scheduleentry_stafftags.upd_dt
	from {{ source('qgenda_ods', 'scheduleentry_stafftags') }} as scheduleentry_stafftags
	inner join {{ ref('stg_qgenda_scc_schedule_entry_base') }} as schedule_entry
		on schedule_entry.schedulekey = scheduleentry_stafftags.schedulekey
	where stafftags_categoryname = 'Specialty'
),

staffkey_union as (
	select
		staffmember_tags.staffkey,
		tags_tags_name as specialty
	from {{ source('qgenda_ods', 'staffmember_tags') }} as staffmember_tags
	inner join {{ source('qgenda_ods', 'staffmember') }} as staffmember
		on staffmember.staffkey = staffmember_tags.staffkey
		and staffmember.compkey = '1121178a-aa59-4654-9160-043975c9fff1'
	where tags_categoryname = 'Specialty'

	union all

	select
		staffkey,
		coalesce(specialty_correction, stafftags_tags_name) as specialty
	from specialty_lead
	group by
		staffkey,
		specialty
)

select
	staffkey_union.staffkey,
	staffmember.abbrev,
	case
		when specialty in (
			'HEMATOLOGY ONCOLOGY',
			'HEMATOLOGY'
		) then 'ONCOLOGY'
		when specialty = 'ANESTHESIA RESOURCE CENTER' then 'GENERAL ANESTHESIA'
		else specialty
	end as specialty
from staffkey_union
left join {{ source('qgenda_ods', 'staffmember') }} as staffmember
	on staffkey_union.staffkey = staffmember.staffkey
where specialty not in (
	'PRIMARY CARE',
	'URGENT CARE'
)
group by
	staffkey_union.staffkey,
	staffmember.abbrev,
	specialty
