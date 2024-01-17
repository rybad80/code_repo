-- "Check columns" are the columns that are used in the hash value to check for changes.
-- These are the non-key and non-audit columns. Use single quotes.
{% set check_columns = ['supply_id', 'supply_name', 'unit_type', 'unit_type_id', 'item_type', 
                'item_type_id', 'primary_external_id'] %}
 
{{ config(
    materialized = 'incremental',
    unique_key = 'supply_key',
    incremental_strategy = 'merge',
    merge_update_columns = check_columns + ['hash_value', 'integration_id', 'update_date',
        'update_source'],
    meta = {
        'critical': true
    }
) }}

with combine_sources as (
    select
        or_sply.supply_id,
        or_sply.supply_name,
        cast(or_sply.type_of_item_c as numeric) as item_type_id,
        zc_or_type_of_item.name as item_type,
        cast(or_sply.order_pack_type_c as numeric) as unit_type_id,
        zc_or_unit_issue.name as unit_type,
        cast(or_sply.primary_ext_id as varchar(50)) as primary_external_id,
        {{
            dbt_utils.surrogate_key(check_columns or [] )
        }} as hash_value,
        'CLARITY' || '~' || supply_id as integration_id,
        current_timestamp as create_date,
        'CLARITY' as create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        {{source('clarity_ods','or_sply')}} as or_sply
        left join {{source('clarity_ods','zc_or_type_of_item')}} as zc_or_type_of_item
            on zc_or_type_of_item.type_of_item_c = or_sply.type_of_item_c
        left join {{source('clarity_ods','zc_or_unit_issue')}} as zc_or_unit_issue
           on zc_or_unit_issue.unit_issue_c = or_sply.order_pack_type_c
    
    union all

    select
        supply_id,
        supply_nm as supply_name,
        null as item_type_id,
        null as item_type,
        unit_type_id,
        unit_type,
        NULL as primary_external_id,
        {{
            dbt_utils.surrogate_key(check_columns or [] )
        }} as hash_value,
        'ORM' || '~' || supply_id as integration_id,
        current_timestamp as create_date,
        'ORM' as create_source,
        current_timestamp as update_date,
        'ORM' as update_source
    from
        {{source('manual_ods','orm_or_supply')}}
),

unionset as (
    select
        {{
            dbt_utils.surrogate_key([
                'supply_id',
                'create_source'
            ])
        }} as supply_key,
        supply_id,
        supply_name,
        item_type_id,
        item_type,
        unit_type_id,
        unit_type,
        primary_external_id,
        hash_value,
        integration_id,
        create_date,
        create_source,
        update_date,
        update_source
    from
        combine_sources

    union all

    select
        -1 as supply_key,
        -1 as supply_id,
        'UNSPECIFIED' as supply_name,
        -1 as item_type_id,
        'UNSPECIFIED' as item_type,
        -1 as unit_type_id,
        'UNSPECIFIED' as unit_type,
        null as primary_external_id,
        -1 as hash_value,
        'UNSPECIFIED' as integration_id,
        current_timestamp as create_date,
        'UNSPECIFIED' as create_source,
        current_timestamp as update_date,
        'UNSPECIFIED' as update_source

    union all

    select
        -2 as supply_key,
        -2 as supply_id,
        'NOT APPLICABLE' as supply_name,
        -2 as item_type_id,
        'NOT APPLICABLE' as item_type,
        -2 as unit_type_id,
        'NOT APPLICABLE' as unit_type,
        null as primary_external_id,
        -2 as hash_value,
        'NOT APPLICABLE' as integration_id,
        current_timestamp as create_date,
        'NOT APPLICABLE' as create_source,
        current_timestamp as update_date,
        'NOT APPLICABLE' as update_source

)

select
    supply_key,
    supply_id,
    supply_name,
    item_type_id,
    item_type,
    unit_type_id,
    unit_type,
    primary_external_id,
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
        and unionset.hash_value not in
        (
            select
                hash_value
            from
                {{ this }} -- TDL dim table
            where integration_id = unionset.integration_id
        )
    {%- endif %}