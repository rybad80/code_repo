{{
    config(
        materialized = 'incremental',
        unique_key = 'location_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['location_wid', 'location_id', 'effective_date', 'location_name', 'inactive_ind', 'latitude', 'longitude', 'altitude', 'external_name', 'worksite_id_code', 'global_location_number', 'location_identifier', 'location_type_wid', 'location_type_id', 'time_profile_wid', 'time_profile_id', 'time_zone_wid', 'time_zone_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with location_data as (
    select distinct
        get_locations.location_reference_wid as location_wid,
        get_locations.location_reference_location_id as location_id,
        to_timestamp(null,'mm/dd/yyyy hh24:mi:ss.us') as effective_date,
        substr(get_locations.location_data_location_name,1,100) as location_name,
        cast(get_locations.location_data_inactive as int) as inactive_ind,
        cast(get_locations.location_data_latitude as numeric(10,4)) as latitude,
        cast(get_locations.location_data_longitude as numeric(10,4)) as longitude,
        cast(get_locations.location_data_altitude as numeric(10,4)) as altitude,
        null as external_name,
        null as worksite_id_code,
        null as global_location_number,
        null as location_identifier,
        get_locations.location_type_reference_wid as location_type_wid,
        get_locations.location_type_reference_location_type_id as location_type_id,
        get_locations.time_profile_reference_wid as time_profile_wid,
        get_locations.time_profile_reference_time_profile_id as time_profile_id,
        get_locations.time_zone_reference_wid as time_zone_wid,
        get_locations.time_zone_reference_time_zone_id as time_zone_id,
        cast({{
            dbt_utils.surrogate_key([
            'location_wid',
             'location_id',
             'effective_date',
             'location_name',
             'inactive_ind',
             'latitude',
             'longitude',
             'altitude',
             'external_name',
             'worksite_id_code',
             'global_location_number',
             'location_identifier',
             'location_type_wid',
             'location_type_id',
             'time_profile_wid',
             'time_profile_id',
             'time_zone_wid',
             'time_zone_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_locations')}} as get_locations
)
select
    location_wid,
    location_id,
    effective_date,
    location_name,
    inactive_ind,
    latitude,
    longitude,
    altitude,
    external_name,
    worksite_id_code,
    global_location_number,
    location_identifier,
    location_type_wid,
    location_type_id,
    time_profile_wid,
    time_profile_id,
    time_zone_wid,
    time_zone_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    location_data
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                location_wid = location_data.location_wid
        )
    {%- endif %}