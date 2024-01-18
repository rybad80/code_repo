{{
    config(
        materialized = 'incremental',
        unique_key = 'job_family_group_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['job_family_group_wid', 'job_family_group_id', 'effective_date', 'name', 'summary', 'inactive_ind', 'md5', 'upd_dt', 'upd_by']
    )
}}
with family_group as (
    select distinct
        get_job_family_groups.job_family_group_reference_wid as job_family_group_wid,
        get_job_family_groups.job_family_group_reference_job_family_id as job_family_group_id,
        to_timestamp(get_job_family_groups.job_family_group_data_effective_date, 'yyyy-mm-dd') as effective_date,
        get_job_family_groups.job_family_group_data_name as name,
        null as summary,
        coalesce(cast(get_job_family_groups.job_family_group_data_inactive as int), -2) as inactive_ind,
        cast({{
            dbt_utils.surrogate_key([
                'job_family_group_wid',
                'job_family_group_id',
                'effective_date',
                'name',
                'summary',
                'inactive_ind',
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_job_family_groups')}} as get_job_family_groups
)
select
    job_family_group_wid,
    job_family_group_id,
    effective_date,
    name,
    summary,
    inactive_ind,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    family_group
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                job_family_group_wid = family_group.job_family_group_wid
        )
    {%- endif %}
