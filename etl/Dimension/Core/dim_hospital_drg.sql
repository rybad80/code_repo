{% set column_names = ['drg_id', 'drg_name', 'drg_number'] %}

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
                'drg_id',
                '\'ADMIN\''
            ])
        }} as drg_key,
        cast(drg_id as numeric) as drg_id,
        drg_name,
        drg_number,
        drg,
        drg_with_soi,
        {{
            dbt_utils.surrogate_key(column_names or [] )
        }} as hash_value,
        'ADMIN' || '~' || drg_id as integration_id,
        current_timestamp as create_date,
        'ADMIN' as create_source,
        current_timestamp as update_date,
        'ADMIN' as update_source
    from
        {{ref('lookup_additional_drg')}}

    union all

    select
        {{
            dbt_utils.surrogate_key([
                'drg_id',
                '\'CLARITY\''
            ])
        }} as drg_key,
        cast(drg_id as numeric) as drg_id,
        drg_name,
        drg_number,
        cast(
            case
                when substr(drg_number, 1, 3) = 'APR'
                then substr(drg_number, 4, 3)
            end as numeric
        ) as drg,
        cast(
            case
                when substr(drg_number, 1, 3) = 'APR'
                then substr(drg_number, 4, 4)
            end as numeric
        ) as drg_with_soi,
            {{
            dbt_utils.surrogate_key(column_names or [] )
        }} as hash_value,
        'CLARITY' || '~' || drg_id as integration_id,
        current_timestamp as create_date,
        'CLARITY' as create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        {{source('clarity_ods','clarity_drg')}}

    union all

    select
        0 as drg_key,
        0 as drg_id,
        'DEFAULT' as drg_name,
        NULL as drg_number,
        NULL as drg,
        NULL as drg_with_soi,
        0 as hash_value,
        'NA' as integration_id,
        current_timestamp as create_date,
        'DEFAULT' as create_source,
        current_timestamp as update_date,
        'DEFAULT' as update_source

    union all

    select
        -1 as drg_key,
        -1 as drg_id,
        'INVALID' as drg_name,
        NULL as drg_number,
        NULL as drg,
        NULL as drg_with_soi,
        0 as hash_value,
        'NA' as integration_id,
        current_timestamp as create_date,
        'DEFAULT' as create_source,
        current_timestamp as update_date,
        'DEFAULT' as update_source

    union all

    select
        1 as drg_key,
        1 as drg_id,
        'PRINCIPAL DIAGNOSIS INVALID AS DISCHARGE DIAGNOSIS' as drg_name,
        'APR9550' as drg_number,
        955 as drg,
        9550 as drg_with_soi,
        0 as hash_value,
        'NA' as integration_id,
        current_timestamp as create_date,
        'DEFAULT' as create_source,
        current_timestamp as update_date,
        'DEFAULT' as update_source
)

select
    drg_key,
    drg_id,
    drg_name,
    drg_number,
    drg,
    drg_with_soi,
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
