with scc_rooms as (
    select
        qgenda_task.taskkey,
        qgenda_task.name,
        qgenda_task.abbrev,
        case
            when qgenda_task.name like '%)' then upper(substring(
                regexp_extract(
                    qgenda_task.name, '^.*\d+.[[:upper:]]{2}'
                ), 1, length(
                    regexp_extract(
                        qgenda_task.name, '^.*\d+.[[:upper:]]{2}')
                 ) - 3)
            )
            else upper(substring(
                regexp_extract(
                    qgenda_task.name, '^.+\s(?=[^ ]*$)'
                ), 1, length(
                    regexp_extract(
                        qgenda_task.name, '^.+\s(?=[^ ]*$)')
                ) - 1))
        end as room_name,
        coalesce(qgenda_task_tags.tags_tags_name, stg_qgenda_scc_location.location_name) as location_name,
        case
            when stg_qgenda_scc_location.site is null
                and name like 'BGR%'
            then 'BGR'
            when stg_qgenda_scc_location.site is null
                and name not like 'BGR%'
            then qgenda_task_tags.tags_tags_name
            else stg_qgenda_scc_location.site
        end as site,
        case
            when name like '% AM%' then 'AM'
            when name like '% PM%' then 'PM'
            when name like '% EVE%' then 'EVE'
        end as session_type
	from {{ source('qgenda_ods', 'qgenda_task_tags') }} as qgenda_task_tags
	inner join {{ source('qgenda_ods', 'qgenda_task') }} as qgenda_task
		on qgenda_task_tags.taskkey = qgenda_task.taskkey
	left join {{ ref('stg_qgenda_scc_location') }} as stg_qgenda_scc_location
        on stg_qgenda_scc_location.taskkey = qgenda_task.taskkey
	where qgenda_task.compkey = '1121178a-aa59-4654-9160-043975c9fff1'
	and qgenda_task_tags.tags_categoryname = 'Location'
	and abbrev not like 'Close%'
	and abbrev not like 'Hold%'
	group by
        qgenda_task.taskkey,
        tags_tags_name,
        location_name,
        qgenda_task.abbrev,
        qgenda_task.name,
        stg_qgenda_scc_location.site
),

scc_room_type as (
	select
		qgenda_task_tags.taskkey,
        qgenda_task_tags.tags_tags_name as room_type,
        case room_type
            when 'Exam Room' then 1
			when 'Non Exam Room' then 0
			else 0
		end as exam_room_ind
	from {{ source('qgenda_ods', 'qgenda_task_tags') }} as qgenda_task_tags
	where tags_categoryname = 'Room Type'
)

select
    scc_rooms.taskkey,
    scc_rooms.room_name,
    scc_rooms.session_type,
    location_name,
    site,
    scc_room_type.room_type,
    scc_room_type.exam_room_ind
from scc_rooms
left join scc_room_type
    on scc_room_type.taskkey = scc_rooms.taskkey
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
    scc_rooms.taskkey,
    scc_rooms.room_name,
    scc_rooms.session_type,
    location_name,
    site,
    scc_room_type.room_type,
    scc_room_type.exam_room_ind
