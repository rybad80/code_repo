with site_exam_room_count as (
	select
		site,
		location_name,
		room_type,
		count(distinct room_name) as exam_room_count
	from {{ ref('stg_qgenda_scc_rooms') }}
	where room_name is not null
	and room_type = 'Exam Room'
	and ((site != 'BGR' and session_type != 'EVE')
		or location_name in (
			'BGR1',
			'BGR3',
			'BGR4',
			'BGR5',
			'BGR6',
			'BGR7',
			'BGR8',
			'BGR9',
			'BGR10',
			'BGR11',
			'BGR12'
		)
	)
	group by
        site,
		location_name,
        room_type
)

select
	site,
	location_name,
	room_type,
	exam_room_count
from
	site_exam_room_count
