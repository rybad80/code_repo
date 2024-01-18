with worker_phone_data as (
    select distinct
        get_workers_personal_data.worker_worker_reference_wid as worker_wid,
        get_workers_personal_data.worker_worker_data_worker_id as worker_id,
        get_workers_personal_data.worker_worker_data_user_id as user_id,
        get_workers_personal_data.worker_worker_data_universal_id as universal_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_employee_id as int) as varchar(50)) as employee_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_contingent_worker_id as int) as varchar(50)) as contingent_worker_id,
        coalesce(get_workers_personal_data.personal_data_contact_data_phone_data_phone_reference_phone_id, 'N/A') as phone_id,
        get_workers_personal_data.personal_data_contact_data_phone_data_tenant_formatted_phone as formatted_phone,
        -2 as deleted_ind,
        2 as do_not_replace_all_ind,
        get_workers_personal_data.worker_data_personal_data_contact_data_phone_data_country_iso_code as country_iso_code,
        cast(cast(get_workers_personal_data.worker_data_personal_data_contact_data_phone_data_international_phone_code as int) as varchar(50)) as international_phone_code,
        cast(cast(get_workers_personal_data.personal_data_contact_data_phone_data_area_code as int) as varchar(50)) as area_code,
        get_workers_personal_data.worker_data_personal_data_contact_data_phone_data_phone_number as phone_number,
        get_workers_personal_data.worker_data_personal_data_contact_data_phone_data_phone_extension as phone_extension,
        get_workers_personal_data.personal_data_contact_data_phone_data_phone_device_type_reference_phone_device_type_id as phone_device_type_id,
        coalesce(cast(get_workers_personal_data.personal_data_contact_data_phone_data_usage_data_public as int), 0) as public_ind,
        coalesce(cast(get_workers_personal_data.contact_data_phone_data_usage_data_type_data_primary as int), 0) as primary_phone_ind,
        get_workers_personal_data.phone_data_usage_data_type_data_type_reference_communication_usage_type_id as communication_usage_type_id,
        cast({{
            dbt_utils.surrogate_key([
                'worker_wid',
                'phone_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_workers_personal_data')}} as get_workers_personal_data
)
select
    worker_wid,
    worker_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    phone_id,
    formatted_phone,
    deleted_ind,
    do_not_replace_all_ind,
    country_iso_code,
    international_phone_code,
    area_code,
    phone_number,
    phone_extension,
    phone_device_type_id,
    public_ind,
    primary_phone_ind,
    communication_usage_type_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_phone_data
where
    1 = 1
