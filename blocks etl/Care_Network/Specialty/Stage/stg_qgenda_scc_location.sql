with location_base as ( --region transform specific location name for later joining
	select
		schedule_entry.schedulekey,
		schedule_entry.staffkey,
		schedule_entry.taskkey,
		schedule_entry.locationname as location_name
	from {{ ref('stg_qgenda_scc_schedule_entry_base') }} as schedule_entry
	group by
		schedule_entry.schedulekey,
		schedule_entry.staffkey,
		schedule_entry.taskkey,
		location_name
	union all
	select
		schedule_entry.schedulekey,
		schedule_entry.staffkey,
		schedule_entry.taskkey,
		tasktags_tags_name as location_name
	from {{ source('qgenda_ods', 'scheduleentry_tasktags') }} as scheduleentry_tasktags
	inner join {{ ref('stg_qgenda_scc_schedule_entry_base') }} as schedule_entry
		on schedule_entry.schedulekey = scheduleentry_tasktags.schedulekey
	where lower(tasktags_categoryname) = 'location'
	group by
		schedule_entry.schedulekey,
		schedule_entry.staffkey,
		schedule_entry.taskkey,
		location_name
	union all
	select
		scheduleentry.schedulekey,
		scheduleentry.staffkey,
		taskkey,
		locationname as location_name
	from {{ source('qgenda_ods', 'scheduleentry') }} as scheduleentry
	where compkey = '1121178a-aa59-4654-9160-043975c9fff1'
	group by
		schedulekey,
		staffkey,
		taskkey,
		location_name
)

select
	location_base.schedulekey,
	location_base.staffkey,
	location_base.taskkey,
	case
		when location_base.location_name = 'ABG Legacy' then 'ABG-Leg'
		else location_base.location_name
	end as location_name,
	case
		when location_base.location_name = 'ABG Legacy' then 'ABG-Leg'
		when location_base.location_name like 'BGR%' then 'BGR'
		else location_base.location_name
	end as site
from location_base
where site in (
	'ABG',
	'ABG-Leg',
	'ATL',
	'BGR',
	'BMW',
	'BUC',
	'BWV',
	'EXT',
	'LGH',
	'LGH-Leg',
	'KOP',
	'PNJ',
	'VIR',
	'VNJ'
)
group by
	location_base.schedulekey,
	location_base.staffkey,
	location_base.taskkey,
	location_base.location_name,
	site
