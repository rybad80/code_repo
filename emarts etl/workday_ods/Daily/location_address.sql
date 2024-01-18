{{
    config(
        materialized = 'incremental',
        unique_key = ['location_wid', 'address_id'],
        incremental_strategy = 'merge',
        merge_update_columns = ['location_wid', 'location_id', 'formatted_address', 'address_format_type', 'defaulted_business_site_address_ind', 'deleted_ind', 'do_not_replace_all_ind', 'effective_date', 'last_modified_date', 'municipality', 'country_region_descriptor', 'postal_code', 'municipality_local', 'number_of_days', 'address_id', 'address_line_1', 'address_line_2', 'country_iso_3166_1_alpha_2_cd', 'country_iso_3166_1_alpha_3_cd', 'country_region_iso_3166_2_cd', 'country_region_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with location_address_data as (
    select distinct
        get_locations.location_reference_wid as location_wid,
        get_locations.location_reference_location_id as location_id,
        get_locations.formatted_address as formatted_address,
        get_locations.address_format_type as address_format_type,
        coalesce(cast(get_locations.defaulted_business_site_address as int), -2) as defaulted_business_site_address_ind,
        -2 as deleted_ind,
        -2 as do_not_replace_all_ind,
        to_timestamp(get_locations.effective_date, 'yyyy-mm-dd') as effective_date,
        to_timestamp((substring(get_locations.address_data_last_modified, 1, 10) || ' ' || substring(get_locations.address_data_last_modified, 12, 8)), 'yyyy-mm-dd hh:mi:ss') as last_modified_date,
        get_locations.address_data_municipality as municipality,
        get_locations.address_data_country_region_descriptor as country_region_descriptor,
        case when get_locations.address_data_postal_code not like '%-%' then substring(get_locations.address_data_postal_code, 1, 5) else get_locations.address_data_postal_code end as postal_code,
        null as municipality_local,
        cast(get_locations.address_data_number_of_days as numeric(30,2)) as number_of_days,
        coalesce(get_locations.address_reference_address_id, '-2') as address_id,
        get_locations.address_data_address_line_1 as address_line_1,
        get_locations.address_data_address_line_2 as address_line_2,
        get_locations.country_reference_iso_3166_1_alpha_2_code as country_iso_3166_1_alpha_2_cd,
        get_locations.country_reference_iso_3166_1_alpha_3_code as country_iso_3166_1_alpha_3_cd,
        get_locations.country_region_reference_iso_3166_2_code as country_region_iso_3166_2_cd,
        get_locations.country_region_reference_country_region_id as country_region_id,
        cast({{
            dbt_utils.surrogate_key([
                'location_wid',
                'location_id',
                'formatted_address',
                'address_format_type',
                'defaulted_business_site_address_ind',
                'deleted_ind',
                'do_not_replace_all_ind',
                'effective_date',
                'last_modified_date',
                'municipality',
                'country_region_descriptor',
                'postal_code',
                'municipality_local',
                'number_of_days',
                'address_id',
                'address_line_1',
                'address_line_2',
                'country_iso_3166_1_alpha_2_cd',
                'country_iso_3166_1_alpha_3_cd',
                'country_region_iso_3166_2_cd',
                'country_region_id'
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
    formatted_address,
    address_format_type,
    defaulted_business_site_address_ind,
    deleted_ind,
    do_not_replace_all_ind,
    effective_date,
    last_modified_date,
    municipality,
    country_region_descriptor,
    postal_code,
    municipality_local,
    number_of_days,
    address_id,
    address_line_1,
    address_line_2,
    country_iso_3166_1_alpha_2_cd,
    country_iso_3166_1_alpha_3_cd,
    country_region_iso_3166_2_cd,
    country_region_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    location_address_data
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                location_wid = location_address_data.location_wid
                and address_id = location_address_data.address_id
        )
    {%- endif %}
