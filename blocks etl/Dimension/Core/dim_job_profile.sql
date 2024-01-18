{{
  config(
    materialized = 'incremental',
    unique_key = 'job_profile_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['job_profile_id', 'job_profile_wid', 'job_code', 'job_title', 'job_profile_private_title', 'job_profile_summary', 'job_desc', 'additional_job_desc', 'pay_rate_type', 'management_level', 'compensation_grade', 'effective_date', 'inactive_ind', 'include_job_code_in_name_ind', 'work_shift_required_ind', 'public_job_ind', 'critical_job_ind', 'hash_value', 'integration_id', 'update_date'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'job_profile'), except=['pay_rate_type_wid', 'management_level_wid', 'compensation_grade_wid', 'md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with job_family
as (
select
    job_family_key,
    job_family_wid as cte_job_family_wid
from {{ref('dim_job_family')}} as dim_job_family 
),
job_profile
as (
select
    {{
        dbt_utils.surrogate_key([
            'job_profile.job_profile_wid'
        ])
    }} as job_profile_key,
    coalesce(dim_job_family.job_family_key, -1) as job_family_key,
    job_profile.job_profile_id,
    job_profile.job_profile_wid,
    job_profile.job_code,
    job_profile.job_title,
    job_profile.job_profile_private_title,
    job_profile.job_profile_summary,
    job_profile.job_description as job_desc,
    job_profile.additional_job_description as additional_job_desc,
    job_profile.pay_rate_type_id as pay_rate_type,
    job_profile.management_level_id as management_level,
    job_profile.compensation_grade_id as compensation_grade,
    job_profile.effective_date,
    job_profile.inactive_ind,
    job_profile.include_job_code_in_name_ind,
    job_profile.work_shift_required_ind,
    job_profile.public_job_ind,
    job_profile.critical_job_ind,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    job_profile.create_by || '~' || job_profile.job_profile_id as integration_id,
    current_timestamp as create_date,
    job_profile.create_by,
    current_timestamp as update_date,
    job_profile.upd_by as update_by
from
    {{source('workday_ods', 'job_profile')}} as job_profile
    left join job_family as dim_job_family 
       on job_profile.job_family_wid = dim_job_family.cte_job_family_wid
--
union all
--
select
    -1,
    -1,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    NULL,
    0,
    0,
    0,
    0,
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
    -2,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    NULL,
    0,
    0,
    0,
    0,
    0,
    -2,
    'NA',
    CURRENT_TIMESTAMP,
    'NOT APPLICABLE',
    CURRENT_TIMESTAMP, 
    'NOT APPLICABLE'
)
select
    job_profile.job_profile_key,
    job_profile.job_family_key,
    job_profile.job_profile_id,
    job_profile.job_profile_wid,
    job_profile.job_code,
    job_profile.job_title,
    job_profile.job_profile_private_title,
    job_profile.job_profile_summary,
    job_profile.job_desc,
    job_profile.additional_job_desc,
    job_profile.pay_rate_type,
    job_profile.management_level,
    job_profile.compensation_grade,
    job_profile.effective_date,
    job_profile.inactive_ind,
    job_profile.include_job_code_in_name_ind,
    job_profile.work_shift_required_ind,
    job_profile.public_job_ind,
    job_profile.critical_job_ind,
    job_profile.hash_value,
    job_profile.integration_id,
    job_profile.create_date,
    job_profile.create_by,
    job_profile.update_date,
    job_profile.update_by
from
    job_profile
where
    1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where job_profile_wid = job_profile.job_profile_wid)
{%- endif %}
