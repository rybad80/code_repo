select
    {{ dbt_utils.surrogate_key(['business_objects_ads_event.event_id']) }} as bo_content_session_key,
    business_objects_ads_event.object_id,
    business_objects_ads_event.object_name,
    object_type.object_type_id,
    object_type.object_type_name as object_type,
    lower(regexp_extract(business_objects_ads_event.user_name, '\w+$')) as user_name,
    business_objects_ads_event.top_folder_name,
    business_objects_ads_event.object_folder_path as full_folder_path,
    business_objects_ads_event.session_id,
    min(business_objects_ads_event.start_time) as session_start,
    sum(business_objects_ads_event.duration_ms) / 60 as session_duration_secs,
    min(business_objects_ads_event.start_time)::date as usage_date,
    session_start + (session_duration_secs || ' seconds')::interval as session_end,
    business_objects_ads_event.event_id,
    {{
        dbt_utils.surrogate_key([
            'business_objects_ads_event.object_name',
            'object_type.object_type_id',
            'business_objects_ads_event.object_folder_path'
        ])
    }} as bo_content_key,
    {{ dbt_utils.surrogate_key([ "'business objects'", 'bo_content_key']) }} as asset_inventory_key
from
    {{ source('business_objects_ods', 'business_objects_ads_event') }} as business_objects_ads_event
    inner join {{ source('business_objects_ods', 'business_objects_ads_object_type_str') }} as object_type
        on object_type.object_type_id = business_objects_ads_event.object_type_id
    inner join {{ source('business_objects_ods', 'business_objects_ads_event_type_str') }} as event_type
        on event_type.event_type_id = business_objects_ads_event.event_type_id
where
    business_objects_ads_event.object_id not in ('0', 'NULL')
    and business_objects_ads_event.object_name is not null
    and business_objects_ads_event.user_name not in ('Administrator', 'SYSTEM ACCOUNT')
    and business_objects_ads_event.top_folder_name in ('CDW', 'Epic Reporting')
    and object_type.language = 'EN'
    and event_type.language = 'EN'
    and (
        (
            object_type.object_type_id in (
                'AUXKVhTVAHhMqij3GUk7jLY', -- word
                'AVSAUQfHHtpFsd9M2RM.qJ0', -- text
                'AVx_3364fDBFni_bGCHtSyQ', -- excel
                'AfgjgipqTrhDvWQR_nzdKBk'  -- pdf
            )
            and event_type.event_type_id = '1013' -- retrieve
        )
        or (
            object_type.object_type_id = 'AURH1uVOzUZDp8QTF2KnXmk' -- crystal
            and event_type.event_type_id in (
                '1002', -- view
                '1003'  -- refresh
            )
        )
        or (
            object_type.object_type_id = 'AYfjfcAV7cNPh33akDfm2RE' -- webi
            and event_type.event_type_id in (
                '1002', -- view
                '1003', -- refresh
                '1010'  -- edit
            )
        )
    )
group by
    business_objects_ads_event.event_id,
    business_objects_ads_event.object_id,
    business_objects_ads_event.object_name,
    object_type.object_type_id,
    object_type.object_type_name,
    lower(regexp_extract(business_objects_ads_event.user_name, '\w+$')),
    business_objects_ads_event.top_folder_name,
    business_objects_ads_event.object_folder_path,
    business_objects_ads_event.session_id
