{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['job_family_wid', 'job_family_id', 'effective_date', 'job_family_name', 'summary', 'inactive_ind', 'job_family_group_wid', 'job_family_group_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'job_family'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with job_family
as (
select
    {{
        dbt_utils.surrogate_key([
            'job_family.job_family_wid'
        ])
    }} as job_family_key,
    job_family.job_family_wid,
    job_family.job_family_id,
    job_family.effective_date,
    job_family.name as job_family_name,
    job_family.summary,
    job_family.inactive_ind,
    coalesce(dim_job_family_group.job_family_group_key, 0) as job_family_group_key,
    job_family.job_family_group_wid,
    job_family.job_family_group_id,
    {{
        dbt_utils.surrogate_key([
            'job_family.job_family_wid',
            'job_family.job_family_id',
            'job_family.effective_date',
            'job_family.name',
            'job_family.summary',
            'job_family.inactive_ind',
            'job_family.job_family_group_wid',
            'job_family.job_family_group_id'
        ])
    }} as hash_value,
    job_family.create_by || '~' || job_family.job_family_wid as integration_id,
    current_timestamp as create_date,
    job_family.create_by,
    current_timestamp as update_date,
    job_family.upd_by as update_by
from
    {{source('workday_ods', 'job_family')}} as job_family
    left join {{ref('dim_job_family_group')}} as dim_job_family_group
       on job_family.job_family_group_wid = dim_job_family_group.job_family_group_wid
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
    'NA',
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
    'NA',
    -2,
    'NA',
    CURRENT_TIMESTAMP,
    'NOT APPLICABLE',
    CURRENT_TIMESTAMP,
    'NOT APPLICABLE'
)
select
    job_family.job_family_key,
    job_family.job_family_wid,
    job_family.job_family_id,
    job_family.effective_date,
    job_family.job_family_name,
    job_family.summary,
    job_family.inactive_ind,
    job_family.job_family_group_key,
    job_family.job_family_group_wid,
    job_family.job_family_group_id,
    job_family.hash_value,
    job_family.integration_id,
    job_family.create_date,
    job_family.create_by,
    job_family.update_date,
    job_family.update_by
from
    job_family
where
    1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = job_family.integration_id)
{%- endif %}
