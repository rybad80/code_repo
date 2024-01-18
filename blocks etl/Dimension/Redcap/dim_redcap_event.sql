with event_records as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'redcap_events_arms.project_id',
                'redcap_events_arms.arm_id',
                'redcap_events_metadata.event_id'
            ]
        )}} as redcap_event_key,
        redcap_events_metadata.event_id,
        redcap_events_arms.project_id,
        redcap_events_arms.arm_id,
        redcap_events_arms.arm_num,
        redcap_events_arms.arm_name,
        redcap_events_metadata.day_offset as event_day_offset,
        redcap_events_metadata.offset_min as event_offset_min,
        redcap_events_metadata.offset_max as event_offset_max,
        redcap_events_metadata.descrip as event_name
    from
        {{ source('ods_redcap_porter', 'redcap_events_metadata') }}
            as redcap_events_metadata
        left join
            {{ source('ods_redcap_porter', 'redcap_events_arms') }}
                as redcap_events_arms
            on redcap_events_metadata.arm_id = redcap_events_arms.arm_id
),

record_union as (
    select
        redcap_event_key,
        event_id,
        project_id,
        arm_id,
        arm_num,
        arm_name,
        event_day_offset,
        event_offset_min,
        event_offset_max,
        event_name 
    from 
        event_records

    union all

    select 
        -1 as redcap_event_key,
        null as event_id,
        null as project_id,
        null as arm_id,
        null as arm_num,
        null as arm_name,
        null as event_day_offset,
        null as event_offset_min,
        null as event_offset_max,
        'MISSING' as event_name
)

select
    redcap_event_key,
    event_id,
    project_id,
    arm_id,
    arm_num,
    arm_name,
    event_day_offset,
    event_offset_min,
    event_offset_max,
    event_name,
    'REDCAP~' || event_id as integration_id,
    current_timestamp as create_date,
    'REDCAP' as create_source,
    current_timestamp as update_date,
    'REDCAP' as update_source
from
    record_union
