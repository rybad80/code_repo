{{
    config(
        materialized = 'incremental',
        unique_key = 'job_profile_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['job_profile_wid', 'job_profile_id', 'job_code', 'effective_date', 'inactive_ind', 'job_title', 'include_job_code_in_name_ind', 'job_profile_private_title', 'job_profile_summary', 'job_description', 'additional_job_description', 'work_shift_required_ind', 'public_job_ind', 'critical_job_ind', 'job_family_wid', 'job_family_id', 'pay_rate_type_wid', 'pay_rate_type_id', 'management_level_wid', 'management_level_id', 'compensation_grade_wid', 'compensation_grade_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with profiles as (
    select distinct
        get_job_profiles_data.job_profile_job_profile_reference_wid as job_profile_wid,
        get_job_profiles_data.job_profile_job_profile_reference_job_profile_id as job_profile_id,
        get_job_profiles_data.job_profile_job_profile_data_job_code as job_code,
        to_timestamp(get_job_profiles_data.job_profile_job_profile_data_effective_date, 'yyyy-mm-dd') as effective_date,
        coalesce(cast(get_job_profiles_data.job_profile_data_job_profile_basic_data_inactive as int), -2) as inactive_ind,
        cast(get_job_profiles_data.job_profile_data_job_profile_basic_data_job_title as nvarchar(100)) as job_title,
        coalesce(cast(get_job_profiles_data.job_profile_data_job_profile_basic_data_include_job_code_in_name as int), -2) as include_job_code_in_name_ind,
        get_job_profiles_data.job_profile_data_job_profile_basic_data_job_profile_private_title as job_profile_private_title,
        cast(get_job_profiles_data.job_profile_data_job_profile_basic_data_job_profile_summary as nvarchar(255)) as job_profile_summary,
        cast(substr(get_job_profiles_data.job_profile_data_job_profile_basic_data_job_description, 1, 100) as nvarchar(100)) as job_description,
        NULL as additional_job_description,
        coalesce(cast(get_job_profiles_data.job_profile_data_job_profile_basic_data_work_shift_required as int), -2) as work_shift_required_ind,
        coalesce(cast(get_job_profiles_data.job_profile_data_job_profile_basic_data_public_job as int), -2) as public_job_ind,
        coalesce(cast(get_job_profiles_data.job_profile_data_job_profile_basic_data_critical_job as int), -2) as critical_job_ind,
        get_job_profiles_data.job_family_data_job_family_reference_wid as job_family_wid,
        get_job_profiles_data.job_family_data_job_family_reference_job_family_id as job_family_id,
        get_job_profiles_data.job_profile_pay_rate_data_pay_rate_type_reference_wid as pay_rate_type_wid,
        get_job_profiles_data.job_profile_pay_rate_data_pay_rate_type_reference_pay_rate_type_id as pay_rate_type_id,
        get_job_profiles_data.job_profile_basic_data_management_level_reference_wid as management_level_wid,
        get_job_profiles_data.job_profile_basic_data_management_level_reference_management_level_id as management_level_id,
        get_job_profiles_data.job_profile_compensation_data_compensation_grade_reference_wid as compensation_grade_wid,
        get_job_profiles_data.job_profile_compensation_data_compensation_grade_reference_compensation_grade_id as compensation_grade_id,
        cast({{
            dbt_utils.surrogate_key([
                'job_profile_wid',
                'job_profile_id',
                'job_code',
                'effective_date',
                'inactive_ind',
                'job_title',
                'include_job_code_in_name_ind',
                'job_profile_private_title',
                'job_profile_summary',
                'job_description',
                'additional_job_description',
                'work_shift_required_ind',
                'public_job_ind',
                'critical_job_ind',
                'job_family_wid',
                'job_family_id',
                'pay_rate_type_wid',
                'pay_rate_type_id',
                'management_level_wid',
                'management_level_id',
                'compensation_grade_wid',
                'compensation_grade_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_job_profiles_data')}} as get_job_profiles_data
)
select
    job_profile_wid,
    job_profile_id,
    job_code,
    effective_date,
    inactive_ind,
    job_title,
    include_job_code_in_name_ind,
    job_profile_private_title,
    job_profile_summary,
    job_description,
    additional_job_description,
    work_shift_required_ind,
    public_job_ind,
    critical_job_ind,
    job_family_wid,
    job_family_id,
    pay_rate_type_wid,
    pay_rate_type_id,
    management_level_wid,
    management_level_id,
    compensation_grade_wid,
    compensation_grade_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    profiles
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                job_profile_wid = profiles.job_profile_wid
        )
    {%- endif %}