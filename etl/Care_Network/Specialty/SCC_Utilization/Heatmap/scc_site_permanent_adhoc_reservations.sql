with stg_heatmap as (
	select
		schedulekey,
		session_type,
		day_name || ' ' || session_type as day_session_type,
		schedule_entry.site,
		week_start,
		month_name
	from {{ ref('scc_qgenda_schedule_entries') }} as schedule_entry
	inner join {{ ref('stg_week_of_month') }} as stg_week_of_month
		on stg_week_of_month.full_date = schedule_entry.resdate
	where schedule_entry.room_type in ('Exam Room', 'Non Exam Room (Overfill)')
	group by
		schedulekey,
		session_type,
		day_session_type,
		site,
		week_start,
		month_name
)

select
	stg_heatmap.schedulekey,
	schedule_entry.specialty,
	stg_heatmap.site,
	schedule_entry.location_name,
	schedule_entry.staff_abbrev,
	schedule_entry.provider_name,
	month_name,
	schedule_entry.resdate,
	schedule_entry.room_name,
	schedule_entry.room_type,
	stg_heatmap.session_type,
	week_start,
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
	--number_of_reservations,
	exam_room_count,
	scc_site_utilization.room_used_ind
from
	stg_heatmap
inner join {{ ref('scc_qgenda_schedule_entries') }} as schedule_entry
	on stg_heatmap.schedulekey = schedule_entry.schedulekey
left join {{ ref('scc_site_utilization') }} as scc_site_utilization
	on scc_site_utilization.schedulekey = stg_heatmap.schedulekey
inner join {{ ref('stg_scc_site_exam_room_count') }} as stg_scc_site_exam_room_count
	on stg_scc_site_exam_room_count.location_name = schedule_entry.location_name
where stg_heatmap.session_type not like '%EVE%'
