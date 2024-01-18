with worker_emails_data as (
    select distinct
        get_workers_personal_data.worker_worker_reference_wid as worker_wid,
        get_workers_personal_data.worker_worker_data_worker_id as worker_id,
        get_workers_personal_data.personal_data_contact_data_email_address_data_email_reference_email_id as email_id,
        get_workers_personal_data.worker_worker_data_user_id as user_id,
        get_workers_personal_data.worker_worker_data_universal_id as universal_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_employee_id as int) as varchar(50)) as employee_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_contingent_worker_id as int) as varchar(50)) as contingent_worker_id,
        -2 as deleted_ind,
        -2 as do_not_replace_all_ind,
        get_workers_personal_data.worker_data_personal_data_contact_data_email_address_data_email_address as email_address,
        get_workers_personal_data.personal_data_contact_data_email_address_data_usage_data_public as public_ind,
        get_workers_personal_data.contact_data_email_address_data_usage_data_type_data_primary as primary_ind,
        get_workers_personal_data.email_address_data_usage_data_type_data_type_reference_communication_usage_type_id as communication_usage_type_id,
        cast({{
            dbt_utils.surrogate_key([
                'worker_wid',
                'worker_id',
                'email_id',
                'user_id',
                'universal_id',
                'employee_id',
                'contingent_worker_id',
                'deleted_ind',
                'do_not_replace_all_ind',
                'email_address',
                'public_ind',
                'primary_ind',
                'communication_usage_type_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_workers_personal_data')}} as get_workers_personal_data
    where
        get_workers_personal_data.personal_data_contact_data_email_address_data_email_reference_email_id is not null
)
select
    worker_wid,
    worker_id,
    email_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    deleted_ind,
    do_not_replace_all_ind,
    email_address,
    public_ind,
    primary_ind,
    communication_usage_type_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_emails_data
where
    1 = 1
