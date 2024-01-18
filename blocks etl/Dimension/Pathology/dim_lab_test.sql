{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['test_id', 'test_name', 'test_abbreviation'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = ['test_id', 'test_name', 'test_abbr'] %}
with test as (
select
    {{
        dbt_utils.surrogate_key(["'CLARITY'", 'test_id'])
    }} as lab_test_key,
    'CLARITY~' || test_mstr_db_main.test_id as integration_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    test_mstr_db_main.test_id,
    test_mstr_db_main.test_name,
    test_mstr_db_main.test_abbr as test_abbreviation,
    current_timestamp as create_date,
    'CLARITY' as create_source,
    current_timestamp as update_date,
    'CLARITY' as update_source
from
    {{source('clarity_ods', 'test_mstr_db_main')}} as test_mstr_db_main
---
union all
---
select
    0,
    'NA',
    0,
    'NA',
    'NA',
    'NA',
    current_timestamp,
    'DEFAULT',
    current_timestamp,
    'DEFAULT'
)
select
    test.lab_test_key,
    test.integration_id,
    test.hash_value,
    test.test_id,
    test.test_name,
    test.test_abbreviation,
    test.create_date,
    test.create_source,
    test.update_date,
    test.update_source
from
    test
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = test.integration_id)
{%- endif %}
