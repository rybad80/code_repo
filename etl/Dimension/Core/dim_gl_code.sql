{% set column_names = [
    'glcode_id',
    'glcode_name',
    'glcode_code',
    'glcode_wid',
    'organization_type_wid',
    'organization_type_id',
    'organization_subtype_wid',
    'organization_subtype_id',
    'availability_date',
    'last_updated_date',
    'inactive_date',
    'inactive_ind'
] %}

{{ config(
    materialized = 'incremental',
    unique_key = 'glcode_key',
    incremental_strategy = 'merge',
    merge_update_columns = column_names + ['hash_value', 'integration_id', 'update_date',
        'update_source'],
    meta = {
        'critical': true
    }
) }}

with unionset as (
select
    {{
        dbt_utils.surrogate_key([
            'glcode_wid',
            "'WORKDAY'"
        ])
    }} as glcode_key,
    glcode_id,
    glcode_name,
    glcode_code,
    glcode_wid,
    organization_type_wid,
    organization_type_id,
    organization_subtype_wid,
    organization_subtype_id,
    availibility_date as availability_date,  -- correcting a typo
    last_updated_date,
    inactive_date,
    inactive_ind,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    'WORKDAY' || '~' || glcode_wid as integration_id,
    current_timestamp as create_date,
    'WORKDAY' as create_source,
    current_timestamp as update_date,
    'WORKDAY' as update_source
from
    {{ source('workday_ods', 'glcode') }} as glcode

union all

select
    -2 as glcode_key,
    '-2' as glcode_id,
    'NOT APPLICABLE' as glcode_name,
    '-2' as glcode_code,
    '-2' as glcode_wid,
    '-2' as organization_type_wid,
    '-2' as organization_type_id,
    '-2' as organization_subtype_wid,
    '-2' as organization_subtype_id,
    null as availability_date,
    null as last_updated_date,
    null as inactive_date,
    0 as inactive_ind,

    -2 as hash_value,
    'NOT APPLICABLE' as integration_id,
    current_timestamp as create_date,
    'NOT APPLICABLE' as create_source,
    current_timestamp as update_date,
    'NOT APPLICABLE' as update_source

union all

select
    -1 as glcode_key,
    '-1' as glcode_id,
    'UNSPECIFIED' as glcode_name,
    '-1' as glcode_code,
    '-1' as glcode_wid,
    '-1' as organization_type_wid,
    '-1' as organization_type_id,
    '-1' as organization_subtype_wid,
    '-1' as organization_subtype_id,
    null as availability_date,
    null as last_updated_date,
    null as inactive_date,
    0 as inactive_ind,

    -1 as hash_value,
    'UNSPECIFIED' as integration_id,
    current_timestamp as create_date,
    'UNSPECIFIED' as create_source,
    current_timestamp as update_date,
    'UNSPECIFIED' as update_source
)

select
    glcode_key,
    glcode_id,
    glcode_name,
    glcode_code,
    glcode_wid,
    organization_type_wid,
    organization_type_id,
    organization_subtype_wid,
    organization_subtype_id,
    availability_date,
    last_updated_date,
    inactive_date,
    inactive_ind,
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
