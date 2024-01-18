{{
    config(
        materialized = 'incremental',
        unique_key = ['worker_wid', 'position_wid'],
        incremental_strategy = 'merge',
        merge_update_columns = ['worker_wid', 'worker_id', 'user_id', 'universal_id', 'employee_id', 'contingent_worker_id', 'position_wid', 'position_id', 'job_profile_wid', 'job_profile_id', 'worker_type_wid', 'employee_type_id', 'contingent_worker_type_id', 'business_site_location_wid', 'business_site_location_id', 'position_time_type_profile_wid', 'position_time_type_profile_id', 'primary_job_ind', 'effective_date', 'position_title', 'business_title', 'start_date', 'end_employment_date', 'job_exempt_ind', 'scheduled_weekly_hours', 'default_weekly_hours', 'working_time_value', 'full_time_equivalent_percentage', 'exclude_from_headcount_ind', 'federal_withholding_fein', 'regular_paid_equivalent_hours', 'worker_hours_profile_classification', 'end_date', 'pay_through_date', 'mgr_employee_id', 'mgr_employee_wid', 'md5', 'upd_dt', 'upd_by']
    )
}}
with worker_job_data as (
    select distinct
        worker_worker_reference_wid as worker_wid,
        worker_worker_data_worker_id as worker_id,
        worker_worker_data_user_id as user_id,
        worker_worker_data_universal_id as universal_id,
        worker_worker_reference_employee_id as employee_id,
        worker_worker_reference_contingent_worker_id as contingent_worker_id,
        position_data_position_reference_wid as position_wid,
        position_data_position_reference_position_id as position_id,
        job_profile_summary_data_job_profile_reference_wid as job_profile_wid,
        job_profile_summary_data_job_profile_reference_job_profile_id as job_profile_id,
        position_data_worker_type_reference_wid as worker_type_wid,
        position_data_worker_type_reference_employee_type_id as employee_type_id,
        position_data_worker_type_reference_contingent_worker_type_id as contingent_worker_type_id,
        business_site_summary_data_location_reference_wid as business_site_location_wid,
        business_site_summary_data_location_reference_location_id as business_site_location_id,
        business_site_summary_data_time_profile_reference_wid as position_time_type_profile_wid,
        business_site_summary_data_time_profile_reference_time_profile_id as position_time_type_profile_id,
        coalesce(cast(employment_data_worker_job_data_primary_job as int), -2) as primary_job_ind,
        to_timestamp(worker_job_data_position_data_effective_date, 'yyyy-mm-dd') as effective_date,
        worker_job_data_position_data_position_title as position_title,
        worker_job_data_position_data_business_title as business_title,
        to_timestamp(worker_job_data_position_data_start_date, 'yyyy-mm-dd') as start_date,
        cast(null as timestamp) as end_employment_date,
        coalesce(cast(worker_job_data_position_data_job_exempt as int), -2) as job_exempt_ind,
        worker_job_data_position_data_scheduled_weekly_hours as scheduled_weekly_hours,
        worker_job_data_position_data_default_weekly_hours as default_weekly_hours,
        worker_job_data_position_data_working_time_value as working_time_value,
        worker_job_data_position_data_full_time_equivalent_percentage as full_time_equivalent_percentage,
        coalesce(cast(worker_job_data_position_data_exclude_from_headcount as int), -2) as exclude_from_headcount_ind,
        worker_job_data_position_data_federal_withholding_fein as federal_withholding_fein,
        null as regular_paid_equivalent_hours,
        null as worker_hours_profile_classification,
        to_timestamp(worker_job_data_position_data_end_date, 'yyyy-mm-dd') as end_date,
        to_timestamp(worker_job_data_position_data_pay_through_date, 'yyyy-mm-dd') as pay_through_date,
        position_data_manager_as_of_last_detected_manager_change_reference_employee_id as mgr_employee_id,
        position_data_manager_as_of_last_detected_manager_change_reference_wid as mgr_employee_wid,
        cast({{
            dbt_utils.surrogate_key([
                'worker_wid',
                'worker_id',
                'user_id',
                'universal_id',
                'employee_id',
                'contingent_worker_id',
                'position_wid',
                'position_id',
                'job_profile_wid',
                'job_profile_id',
                'worker_type_wid',
                'employee_type_id',
                'contingent_worker_type_id',
                'business_site_location_wid',
                'business_site_location_id',
                'position_time_type_profile_wid',
                'position_time_type_profile_id',
                'primary_job_ind',
                'effective_date',
                'position_title',
                'business_title',
                'start_date',
                'end_employment_date',
                'job_exempt_ind',
                'scheduled_weekly_hours',
                'default_weekly_hours',
                'working_time_value',
                'full_time_equivalent_percentage',
                'exclude_from_headcount_ind',
                'federal_withholding_fein',
                'regular_paid_equivalent_hours',
                'worker_hours_profile_classification',
                'end_date',
                'pay_through_date',
                'mgr_employee_id',
                'mgr_employee_wid'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_workers_job_data')}} as get_workers_job_data
)
select
    worker_wid,
    worker_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    position_wid,
    position_id,
    job_profile_wid,
    job_profile_id,
    worker_type_wid,
    employee_type_id,
    contingent_worker_type_id,
    business_site_location_wid,
    business_site_location_id,
    position_time_type_profile_wid,
    position_time_type_profile_id,
    primary_job_ind,
    effective_date,
    position_title,
    business_title,
    start_date,
    end_employment_date,
    job_exempt_ind,
    scheduled_weekly_hours,
    default_weekly_hours,
    working_time_value,
    full_time_equivalent_percentage,
    exclude_from_headcount_ind,
    federal_withholding_fein,
    regular_paid_equivalent_hours,
    worker_hours_profile_classification,
    end_date,
    pay_through_date,
    mgr_employee_id,
    mgr_employee_wid,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_job_data
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                worker_wid = worker_job_data.worker_wid
                and position_wid = worker_job_data.position_wid
        )
    {%- endif %}
