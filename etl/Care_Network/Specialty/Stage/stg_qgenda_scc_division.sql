with division_tags as (
	select
		scheduleentry_stafftags.schedulekey,
		stafftags_categoryname as division_tags_categoryname,
		stafftags_tags_name as division_tags_name,
        substr(division_tags_name, 0, 3) as location,
		regexp_extract(division_tags_name, '\d+') as division_tags_division_id
	from {{ source('qgenda_ods', 'scheduleentry_stafftags') }} as scheduleentry_stafftags
	inner join {{ ref('stg_qgenda_scc_schedule_entry_base') }} as scheduleentry
		on scheduleentry.schedulekey = scheduleentry_stafftags.schedulekey
	where stafftags_categoryname = 'Division'
),

division_id_tags as (
	select
		scheduleentry_stafftags.schedulekey,
		stafftags_categoryname,
		stafftags_tags_name as division_id
	from {{ source('qgenda_ods', 'scheduleentry_stafftags') }} as scheduleentry_stafftags
	inner join {{ ref('stg_qgenda_scc_schedule_entry_base') }} as scheduleentry
		on scheduleentry.schedulekey = scheduleentry_stafftags.schedulekey
	where stafftags_categoryname = 'Division ID'
)

select
	division_tags.*,
	division_id_tags.stafftags_categoryname,
	division_id_tags.division_id
from division_tags
inner join {{ ref('stg_qgenda_scc_location') }} as stg_qgenda_scc_location
    on division_tags.schedulekey = stg_qgenda_scc_location.schedulekey
    and division_tags.location = stg_qgenda_scc_location.site
left join division_id_tags
	on division_id_tags.schedulekey = division_tags.schedulekey
	and division_tags.division_tags_division_id = division_id_tags.division_id
