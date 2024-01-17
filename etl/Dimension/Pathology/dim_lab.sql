{% set check_columns = ['lab_id', 'lab_name'] %}

{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = check_columns + ['hash_value', 'integration_id', 'update_date', 'update_source'],
    meta = {
        'critical': true
    }
  )
}}
with lab as (
select
    {{
        dbt_utils.surrogate_key(['lab_id', "'CLARITY'"])
    }} as lab_key,
    lab_profile.lab_id,
    lab_profile.lab_name,
    case
        when lab_profile.lab_name like 'RL %' then 0
        else 1
    end as chop_lab_ind,
    {{
        dbt_utils.surrogate_key(check_columns or [] )
    }} as hash_value,
    'CLARITY~' || lab_profile.lab_id as integration_id,
    current_timestamp as create_date,
    'CLARITY' as create_source,
    current_timestamp as update_date,
    'CLARITY' as update_source
from
    {{source('clarity_ods', 'lab_profile')}} as lab_profile
---
union all
---
select
    -1 as lab_key,
    -1 as lab_id,
    'UNSPECIFIED' as lab_name,
    null as chop_lab_ind,
    -1 as hash_value,
    'UNSPECIFIED' as integration_id,
    current_timestamp as create_date,
    'UNSPECIFIED' as create_source,
    current_timestamp as update_date,
    'UNSPECIFIED' as update_source
---
union all
---
select
    -2 as lab_key,
    -2 as lab_id,
    'NOT APPLICABLE' as lab_name,
    null as chop_lab_ind,
    -2 as hash_value,
    'NOT APPLICABLE' as integration_id,
    current_timestamp as create_date,
    'NOT APPLICABLE' as create_source,
    current_timestamp as update_date,
    'NOT APPLICABLE' as update_source
)
select
    lab.lab_key,
    lab.lab_id,
    lab.lab_name,
    lab.chop_lab_ind,
    lab.hash_value,
    lab.integration_id,
    lab.create_date,
    lab.create_source,
    lab.update_date,
    lab.update_source
from
    lab
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = lab.integration_id)
{%- endif %}