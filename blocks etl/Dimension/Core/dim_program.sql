{{
  config(
    materialized = 'incremental',
    unique_key = 'program_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['program_wid', 'program_id', 'program_name', 'include_program_id_in_name_ind', 'program_is_inactive_ind', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'program'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with program
as (
select
    {{
        dbt_utils.surrogate_key([
            'program_wid'
        ])
    }} as program_key,
    program.program_wid,
    program.program_id,
    program.program_name,
    program.include_program_id_in_name_ind,
    program.program_is_inactive_ind,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    program.create_by || '~' || program.program_id as integration_id,
    current_timestamp as create_date,
    program.create_by,
    current_timestamp as update_date,
    program.upd_by as update_by
from
    {{source('workday_ods', 'program')}} as program
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    0,
    0,
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP, 
    'DEFAULT'    
)    
select
    program.program_key,
    program.program_wid,
    program.program_id,
    program.program_name,
    program.include_program_id_in_name_ind,
    program.program_is_inactive_ind,
    program.hash_value,
    program.integration_id,
    program.create_date,
    program.create_by,
    program.update_date,
    program.update_by
from
    program    
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where program_wid = program.program_wid)
{%- endif %}
