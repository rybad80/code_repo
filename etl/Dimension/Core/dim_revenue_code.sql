-- "Check columns" are the columns that are used in the hash value to check for changes.
-- These are the non-key and non-audit columns.
{% set check_columns = ['revenue_code_id', 'revenue_code',
    'revenue_code_name'] %}

{{ config(
    materialized = 'incremental',
    unique_key = 'revenue_code_key',
    incremental_strategy = 'merge',
    merge_update_columns = check_columns + ['hash_value', 'integration_id', 'update_date',
        'update_source'],
    meta = {
        'critical': true
    }
) }}

with main as (
    select
        ub_rev_code_id as revenue_code_id,
        revenue_code,
        revenue_code_name
    from
        {{ source('clarity_ods', 'cl_ub_rev_code') }}
),

unionset as (
    select
        {{
            dbt_utils.surrogate_key([
                'revenue_code_id',
                "'CLARITY'"
            ])
        }} as revenue_code_key,
        *,
        {{
            dbt_utils.surrogate_key(check_columns or [] )
        }} as hash_value,
        'CLARITY' || '~' || revenue_code_id as integration_id,
        current_timestamp as create_date,
        'CLARITY' as create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        main

    union all

    select
        -2 as revenue_code_key,
        '-2' as revenue_code_id,
        '-2' as revenue_code,
        'NOT APPLICABLE' as revenue_code_name,
        -2 as hash_value,
        'NOT APPLICABLE' as integration_id,
        current_timestamp as create_date,
        'NOT APPLICABLE' as create_source,
        current_timestamp as update_date,
        'NOT APPLICABLE' as update_source

    union all

    select
        -1 as revenue_code_key,
        '-1' as revenue_code_id,
        '-1' as revenue_code,
        'UNSPECIFIED' as revenue_code_name,
        -1 as hash_value,
        'UNSPECIFIED' as integration_id,
        current_timestamp as create_date,
        'UNSPECIFIED' as create_source,
        current_timestamp as update_date,
        'UNSPECIFIED' as update_source
)

select
    revenue_code_key,
    revenue_code_id::int as revenue_code_id,
    revenue_code::int as revenue_code,
    revenue_code_name,
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
