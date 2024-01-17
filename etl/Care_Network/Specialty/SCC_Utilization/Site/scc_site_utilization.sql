with visits_by_session as (
	select
		department_id,
		encounter_date,
		am_or_pm,
		count(distinct visit_key) as visit_count,
		sum(telehealth_ind) as telehealth_visits,
		case when telehealth_visits = visit_count
			then 1 else 0 end as telehealth_check
	from {{ ref('stg_specialty_care_encounters') }}
	group by
		department_id,
		encounter_date,
		am_or_pm
)

select
    scc_qgenda_schedule_entries.schedulekey,
    scc_qgenda_schedule_entries.department_id,
    scc_qgenda_schedule_entries.location_name,
    scc_qgenda_schedule_entries.site,
    scc_qgenda_schedule_entries.specialty,
    scc_qgenda_schedule_entries.resdate,
    scc_qgenda_schedule_entries.weekday_name,
    scc_qgenda_schedule_entries.fiscal_year,
    scc_qgenda_schedule_entries.fiscal_quarter,
    scc_qgenda_schedule_entries.month_int,
    scc_qgenda_schedule_entries.room_name,
    scc_qgenda_schedule_entries.room_type,
    scc_qgenda_schedule_entries.session_type,
    count(distinct scc_qgenda_schedule_entries.schedulekey) as reserved_rooms,
    sum(visits_by_session.visit_count) as total_visits,
    sum(visits_by_session.telehealth_visits) as telehealth_visits,
    max(visits_by_session.telehealth_check) as telehealth_check_ind,
    case
        when total_visits is not null and telehealth_check_ind = 0
        then 1 else 0 end as room_used_ind
from {{ ref('scc_qgenda_schedule_entries') }} as scc_qgenda_schedule_entries
left join visits_by_session
    on visits_by_session.am_or_pm = scc_qgenda_schedule_entries.session_type
    and visits_by_session.department_id = scc_qgenda_schedule_entries.department_id
    and visits_by_session.encounter_date = scc_qgenda_schedule_entries.resdate
where (lower(room_type) like '%overfill%'
    or exam_room_ind = 1) -- site-level utilization metrics include exam rooms and overfill rooms
and business_day_ind = 1
and scc_qgenda_schedule_entries.resdate < (current_date - 1)
group by
    scc_qgenda_schedule_entries.schedulekey,
    scc_qgenda_schedule_entries.department_id,
    scc_qgenda_schedule_entries.specialty,
    scc_qgenda_schedule_entries.location_name,
    scc_qgenda_schedule_entries.site,
    scc_qgenda_schedule_entries.resdate,
    scc_qgenda_schedule_entries.weekday_name,
    scc_qgenda_schedule_entries.fiscal_year,
    scc_qgenda_schedule_entries.fiscal_quarter,
    scc_qgenda_schedule_entries.month_int,
    scc_qgenda_schedule_entries.room_name,
    scc_qgenda_schedule_entries.room_type,
    scc_qgenda_schedule_entries.session_type
