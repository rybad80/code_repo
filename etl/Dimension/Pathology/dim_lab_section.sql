{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['lab_section_id', 'lab_section_name', 'lab_section_department_grouper'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = [
    'lab_section.section_id',
    'lab_section.section_name',
    'lookup_lab_section_department_grouper.section_department_grouper'
] %}
with section as (
select
    {{
        dbt_utils.surrogate_key(["'CLARITY'", 'lab_section.section_id'])
    }} as lab_section_key,
    'CLARITY~' || lab_section.section_id as integration_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    lab_section.section_id as lab_section_id,
    lab_section.section_name as lab_section_name,
    coalesce(lookup_lab_section_department_grouper.section_department_grouper, 'NA')
        as lab_section_department_grouper,
    current_timestamp as create_date,
    'CLARITY' as create_source,
    current_timestamp as update_date,
    'CLARITY' as update_source
from
    {{source('clarity_ods', 'lab_section')}} as lab_section
    left join {{ref('lookup_lab_section_department_grouper')}} as lookup_lab_section_department_grouper
        on lab_section.section_id = lookup_lab_section_department_grouper.section_id
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
    section.lab_section_key,
    section.integration_id,
    section.hash_value,
    section.lab_section_id,
    section.lab_section_name,
    section.lab_section_department_grouper,
    section.create_date,
    section.create_source,
    section.update_date,
    section.update_source
from
    section
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = section.integration_id)
{%- endif %}
