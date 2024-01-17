{% set column_names = ['place_of_service_id', 'place_of_service_name', 'place_of_service_abbr', 'gl_prefix',
'place_of_service_type', 'place_of_service_code', 'address_line_1', 'address_line_2', 'city', 'state', 'zip', 'area_code', 'phone',
'location_type', 'rpt_grp_1', 'rpt_grp_9', 'rpt_grp_10', 'record_status'] %}
 
{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = column_names,
      meta = {
      'critical': true
    }
  )
}}
 
with unionset as (
    select
        {{
            dbt_utils.surrogate_key([
                'pos_id',
                "'CLARITY'"
            ])
        }} as place_of_service_key,
        pos_id as place_of_service_id, 
        pos_name as place_of_service_name,
        pos_name_abbr as place_of_service_abbr, 
        gl_prefix,  
        pos_type as place_of_service_type, 
        pos_code as place_of_service_code, 
        address_line_1, 
        address_line_2, 
        city, 
        zc_state.abbr as state, 
        zip, 
        area_code, 
        phone, 
        zc_loc_type.title as location_type, 
        rpt_grp_one as rpt_grp_1, 
        zc_pos_rpt_grp_9.title as rpt_grp_9, 
        zc_pos_rpt_grp_10.title as rpt_grp_10, 
        record_status,
        {{
            dbt_utils.surrogate_key(column_names or [] )
        }} as hash_value, 
        'CLARITY' || '~' || pos_id as integration_id,
        current_timestamp as create_date,
        'CLARITY' as create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        {{source('clarity_ods','clarity_pos')}} as clarity_pos
        left join {{source('clarity_ods','zc_state')}} as zc_state
            on zc_state.state_c = clarity_pos.state_c
        left join {{source('clarity_ods','zc_pos_rpt_grp_9')}} as zc_pos_rpt_grp_9
            on zc_pos_rpt_grp_9.rpt_grp_nine = clarity_pos.rpt_grp_nine
        left join {{source('clarity_ods','zc_pos_rpt_grp_10')}} as zc_pos_rpt_grp_10
            on zc_pos_rpt_grp_10.rpt_grp_ten = clarity_pos.rpt_grp_ten
        left join {{source('clarity_ods','zc_loc_type')}} as zc_loc_type
            on zc_loc_type.loc_type_c = clarity_pos.loc_type_c
    
    union all

    select
        -1 as place_of_service_key,
        -1 as place_of_service_id, 
        'UNSPECIFIED' as place_of_service_name,
        'UNSP' as place_of_service_abbr, 
        NULL as gl_prefix,  
        NULL as place_of_service_type, 
        NULL as place_of_service_code,
        NULL as address_line_1, 
        NULL as address_line_2, 
        NULL as city, 
        NULL as state, 
        NULL as zip, 
        NULL as area_code, 
        NULL as phone, 
        NULL as location_type, 
        NULL as rpt_grp_1, 
        NULL as rpt_grp_9, 
        NULL as rpt_grp_10, 
        NULL as record_status, 
        -1 as hash_value,
        'NA' as integration_id,
        current_timestamp as create_date,
        'UNSPECIFIED' as create_source,
        current_timestamp as update_date,
        'UNSPECIFIED' as update_source

    union all

    select
        -2 as place_of_service_key,
        -2 as place_of_service_id, 
        'NOT APPLICABLE' as place_of_service_name,
        'NA' as place_of_service_abbr, 
        NULL as gl_prefix,   
        NULL as place_of_service_type, 
        NULL as place_of_service_code,
        NULL as address_line_1, 
        NULL as address_line_2, 
        NULL as city, 
        NULL as state, 
        NULL as zip, 
        NULL as area_code, 
        NULL as phone, 
        NULL as location_type, 
        NULL as rpt_grp_1, 
        NULL as rpt_grp_9, 
        NULL as rpt_grp_10, 
        NULL as record_status, 
        -2 as hash_value,
        'NA' as integration_id,
        current_timestamp as create_date,
        'NOT APPLICABLE' as create_source,
        current_timestamp as update_date,
        'NOT APPLICABLE' as update_source
 )
 
select
    place_of_service_key,
    place_of_service_id, 
    place_of_service_name,
    place_of_service_abbr, 
    gl_prefix,   
    place_of_service_type, 
    place_of_service_code,
    address_line_1, 
    address_line_2, 
    city, 
    state, 
    zip, 
    area_code, 
    phone, 
    location_type, 
    rpt_grp_1, 
    rpt_grp_9, 
    rpt_grp_10, 
    record_status, 
    hash_value,
    integration_id,
    create_date,
    create_source,
    update_date,
    update_source
from
    unionset
where
    1 = 1
    {%- if is_incremental() %}
        and hash_value not in
        (
            select
                hash_value
            from
                {{ this }} -- TDL dim table
            where integration_id = unionset.integration_id
        )
    {%- endif %}