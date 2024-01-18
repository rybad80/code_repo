with stg_heatmap as (
	select
		schedule_entry.schedulekey,
		schedule_entry.session_type,
		day_name || ' ' || session_type as day_session_type,
		schedule_entry.site,
		week_number
	from {{ ref('scc_qgenda_schedule_entries') }} as schedule_entry
	inner join {{ ref('stg_week_of_month') }} as stg_week_of_month
		on stg_week_of_month.full_date = schedule_entry.resdate
	inner join {{ source('qgenda_ods', 'scheduleentry') }} as scheduleentry
		on schedule_entry.schedulekey = scheduleentry.schedulekey
	and schedule_entry.resdate between last_day(add_months(current_date, 0)) + 1
		and last_day(add_months(current_date, 1))
	and schedule_entry.room_type in ('Exam Room')
	and scheduleentry.islocked = 'false'
	group by
		schedule_entry.schedulekey,
		schedule_entry.session_type,
		day_session_type,
		schedule_entry.site,
		week_number
)

select
	stg_heatmap.schedulekey,
	schedule_entry.specialty,
	stg_heatmap.site,
	schedule_entry.location_name,
	schedule_entry.staff_abbrev,
	schedule_entry.provider_name,
	week_number,
	schedule_entry.resdate,
	schedule_entry.room_name,
	schedule_entry.room_type,
	case
		when day_session_type = 'MONDAY AM' then '(1) MONDAY AM'
		when day_session_type = 'MONDAY PM' then '(1) MONDAY PM'
		when day_session_type = 'TUESDAY AM' then '(2) TUESDAY AM'
		when day_session_type = 'TUESDAY PM' then '(2) TUESDAY PM'
		when day_session_type = 'WEDNESDAY AM' then '(3) WEDNESDAY AM'
		when day_session_type = 'WEDNESDAY PM' then '(3) WEDNESDAY PM'
		when day_session_type = 'THURSDAY AM' then '(4) THURSDAY AM'
		when day_session_type = 'THURSDAY PM' then '(4) THURSDAY PM'
		when day_session_type = 'FRIDAY AM' then '(5) FRIDAY AM'
		when day_session_type = 'FRIDAY PM' then '(5) FRIDAY PM'
		when day_session_type = 'SATURDAY AM' then '(6) SATURDAY AM'
		when day_session_type = 'SATURDAY PM' then '(6) SATURDAY PM'
		else day_session_type
	end as sessions,
	exam_room_count
from
	stg_heatmap
inner join {{ ref('scc_qgenda_schedule_entries') }} as schedule_entry
	on stg_heatmap.schedulekey = schedule_entry.schedulekey
inner join {{ ref('stg_scc_site_exam_room_count') }} as stg_scc_site_exam_room_count
	on stg_scc_site_exam_room_count.location_name = schedule_entry.location_name
where
	stg_heatmap.session_type not like '%EVE%'
