select
    {{
        dbt_utils.surrogate_key([
            'or_util_block.snapshot_date',
            'or_util_block.snapshot_number',
            'or_util_block.room_id',
            'or_util_block.line'
        ])
    }} as surgery_block_util_key,
    to_char(or_util_block.snapshot_date, 'yyyymmdd')::bigint as snapshot_dt_key,
    surgery_case.or_key as case_key,
    surgery_log.or_key as log_key,
    to_char(or_util_block.snapshot_date, 'YYYY-MM-DD') as snapshot_date,
    or_util_block.snapshot_date + to_char(or_util_block.start_time, 'HH24:MI:SS') as slot_start_time,
    case
        when date(or_util_block.end_time) = '1900-01-02'
        then or_util_block.snapshot_date + interval '1 day' + to_char(or_util_block.end_time, 'HH24:MI:SS')
        else or_util_block.snapshot_date + to_char(or_util_block.end_time, 'HH24:MI:SS')
    end as slot_end_time,
    or_util_block.slot_length as slot_length_mins,
    or_util_block.snapshot_number as snapshot_id,
    zc_snapshot_number.title as snapshot_name,
    or_util_block.case_id,
    or_util_block.log_id,
    or_util_block.group_id,
    or_grp.group_name,
    room_loc.prov_id as room_id,
    room_loc.prov_name as room,
    room_loc.location_id,
    room_loc.location,
    surgeon.prov_id as surgeon_id,
    surgeon.prov_name as surgeon_name,
    or_util_block.service_c as service_id,
    zc_or_service.title as service_name,
    or_util_block.line,
    or_util_block.slot_type,
    case when or_util_block.proc_time = 'Y' then 1 else 0 end as proc_time_ind
from
    {{ source('clarity_ods', 'or_util_block') }} as or_util_block
    left join {{ ref('stg_surgery_keys_xref') }} as surgery_log
        on or_util_block.log_id = surgery_log.or_id
        and surgery_log.src_id = 1
        and surgery_log.source_system = 'CLARITY'
    left join {{ ref('stg_surgery_keys_xref') }} as surgery_case
        on or_util_block.case_id = surgery_case.or_id
        and surgery_case.src_id = 2
        and surgery_case.source_system = 'CLARITY'
    inner join {{ source('clarity_ods', 'zc_snapshot_number') }} as zc_snapshot_number
        on or_util_block.snapshot_number = zc_snapshot_number.snapshot_number
    left join {{ ref('stg_surgery_ser_loc') }} as room_loc
        on or_util_block.room_id = room_loc.prov_id
        and room_loc.staff_resource_c = 2 --Resource
    left join {{ ref('stg_surgery_ser_loc') }} as surgeon
        on or_util_block.surgeon_id = surgeon.prov_id
        and or_util_block.line = surgeon.line
        and room_loc.staff_resource_c = 1 --Person
    left join {{ source('clarity_ods', 'or_grp') }} as or_grp
        on or_util_block.group_id = or_grp.group_id
    left join {{ source('clarity_ods', 'zc_or_service') }} as zc_or_service
        on or_util_block.service_c = zc_or_service.service_c
