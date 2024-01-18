{{
    config(
        materialized = 'incremental',
        unique_key = 'position_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['position_wid', 'position_id', 'job_posting_title', 'job_description_summary', 'job_description', 'available_for_hire_ind', 'available_for_recruiting_ind', 'hiring_freeze_ind', 'work_shift_required_ind', 'available_for_overlap_ind', 'earliest_overlap_date', 'critical_job_ind', 'academic_tenure_eligible_ind', 'effective_date', 'closed_ind', 'supervisory_organization_wid', 'supervisory_organization_id', 'job_profile_wid', 'job_profile_id', 'location_wid', 'location_id', 'worker_type_wid', 'worker_type_id', 'position_time_type_wid', 'position_time_type_id', 'position_worker_type_wid', 'employee_type_id', 'contingent_worker_type_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with positions as (
    select distinct
        get_positions.position_position_reference_wid as position_wid,
        get_positions.position_position_reference_position_id as position_id,
        cast(get_positions.position_data_position_definition_data_job_posting_title as nvarchar(150)) as job_posting_title,
        cast(get_positions.position_data_position_definition_data_job_description_summary as nvarchar(255)) as job_description_summary,
        cast(get_positions.position_data_position_definition_data_job_description as nvarchar(150)) job_description,
        coalesce(cast(cast(get_positions.position_data_position_definition_data_available_for_hire as int) as int), -2) as available_for_hire_ind,
        coalesce(cast(get_positions.position_data_position_definition_data_available_for_recruiting as int), -2) as available_for_recruiting_ind,
        coalesce(cast(get_positions.position_data_position_definition_data_hiring_freeze as int), -2) as hiring_freeze_ind,
        coalesce(cast(get_positions.position_data_position_definition_data_work_shift_required as int), -2) as work_shift_required_ind,
        coalesce(cast(get_positions.position_data_position_definition_data_available_for_overlap as int), -2) as available_for_overlap_ind,
        to_timestamp(get_positions.position_data_position_definition_data_earliest_overlap_date, 'yyyy-mm-dd') as earliest_overlap_date,
        coalesce(cast(get_positions.position_data_position_definition_data_critical_job as int), -2) as critical_job_ind,
        coalesce(cast(get_positions.position_data_position_definition_data_academic_tenure_eligible as int), -2) as academic_tenure_eligible_ind,
        to_timestamp(get_positions.position_position_data_effective_date, 'yyyy-mm-dd') as effective_date,
        coalesce(cast(get_positions.position_position_data_closed as int), -2) as closed_ind,
        get_positions.position_data_supervisory_organization_reference_wid as supervisory_organization_wid,
        case when get_positions.position_data_supervisory_organization_reference_organization_reference_id like '%.%' then cast(cast(get_positions.position_data_supervisory_organization_reference_organization_reference_id as int) as varchar(150)) else get_positions.position_data_supervisory_organization_reference_organization_reference_id end as supervisory_organization_id,
        get_positions.job_profile_restriction_summary_data_job_profile_reference_wid as job_profile_wid,
        get_positions.job_profile_restriction_summary_data_job_profile_reference_job_profile_id as job_profile_id,
        get_positions.position_restrictions_data_location_reference_wid as location_wid,
        get_positions.position_restrictions_data_location_reference_location_id as location_id,
        get_positions.position_restrictions_data_worker_type_reference_wid as worker_type_wid,
        get_positions.position_restrictions_data_worker_type_reference_worker_type_id as worker_type_id,
        get_positions.position_restrictions_data_time_type_reference_wid as position_time_type_wid,
        get_positions.position_restrictions_data_time_type_reference_position_time_type_id as position_time_type_id,
        get_positions.position_restrictions_data_position_worker_type_reference_wid as position_worker_type_wid,
        get_positions.position_restrictions_data_position_worker_type_reference_employee_type_id as employee_type_id,
        get_positions.position_restrictions_data_position_worker_type_reference_contingent_worker_type_id as contingent_worker_type_id,
        cast({{
            dbt_utils.surrogate_key([
                'position_wid',
                'position_id',
                'job_posting_title',
                'job_description_summary',
                'job_description',
                'available_for_hire_ind',
                'available_for_recruiting_ind',
                'hiring_freeze_ind',
                'work_shift_required_ind',
                'available_for_overlap_ind',
                'earliest_overlap_date',
                'critical_job_ind',
                'academic_tenure_eligible_ind',
                'effective_date',
                'closed_ind',
                'supervisory_organization_wid',
                'supervisory_organization_id',
                'job_profile_wid',
                'job_profile_id',
                'location_wid',
                'location_id',
                'worker_type_wid',
                'worker_type_id',
                'position_time_type_wid',
                'position_time_type_id',
                'position_worker_type_wid',
                'employee_type_id',
                'contingent_worker_type_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_positions')}} as get_positions
)
select
    position_wid,
    position_id,
    job_posting_title,
    job_description_summary,
    job_description,
    available_for_hire_ind,
    available_for_recruiting_ind,
    hiring_freeze_ind,
    work_shift_required_ind,
    available_for_overlap_ind,
    earliest_overlap_date,
    critical_job_ind,
    academic_tenure_eligible_ind,
    effective_date,
    closed_ind,
    supervisory_organization_wid,
    supervisory_organization_id,
    job_profile_wid,
    job_profile_id,
    location_wid,
    location_id,
    worker_type_wid,
    worker_type_id,
    position_time_type_wid,
    position_time_type_id,
    position_worker_type_wid,
    employee_type_id,
    contingent_worker_type_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    positions
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                position_wid = positions.position_wid
        )
    {%- endif %}