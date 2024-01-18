{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['job_family_group_wid', 'job_family_group_id', 'effective_date', 'job_family_group_name', 'summary', 'inactive_ind', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'job_family_group'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with job_family_group
as (
select
    {{
        dbt_utils.surrogate_key([
            'job_family_group.job_family_group_wid'
        ])
    }} as job_family_group_key,
    job_family_group.job_family_group_wid,
    job_family_group.job_family_group_id,
    job_family_group.effective_date,
    job_family_group.name as job_family_group_name,
    job_family_group.summary,
    job_family_group.inactive_ind,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    job_family_group.create_by || '~' || job_family_group.job_family_group_wid as integration_id,
    current_timestamp as create_date,
    job_family_group.create_by,
    current_timestamp as update_date,
    job_family_group.upd_by as update_by
from
    {{source('workday_ods', 'job_family_group')}} as job_family_group
--
union all
--
select
    -1,
    'NA',
    'NA',
    NULL,
    'NA',
    'NA',
    0,
    -1,
    'NA',
    CURRENT_TIMESTAMP,
    'UNSPECIFIED',
    CURRENT_TIMESTAMP, 
    'UNSPECIFIED'
--
union all
--
select
    -2,
    'NA',
    'NA',
    NULL,
    'NA',
    'NA',
    0,
    -2,
    'NA',
    CURRENT_TIMESTAMP,
    'NOT APPLICABLE',
    CURRENT_TIMESTAMP, 
    'NOT APPLICABLE'
)
select
    job_family_group.job_family_group_key,
    job_family_group.job_family_group_wid,
    job_family_group.job_family_group_id,
    job_family_group.effective_date,
    job_family_group.job_family_group_name,
    job_family_group.summary,
    job_family_group.inactive_ind,
    job_family_group.hash_value,
    job_family_group.integration_id,
    job_family_group.create_date,
    job_family_group.create_by,
    job_family_group.update_date,
    job_family_group.update_by
from
    job_family_group
where
    1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = job_family_group.integration_id)
{%- endif %}
