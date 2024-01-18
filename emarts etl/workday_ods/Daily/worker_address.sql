{{
    config(
        materialized = 'incremental',
        unique_key = ['worker_wid', 'address_id', 'communication_usage_behavior_id', 'communication_usage_type_id', 'effective_date'],
        incremental_strategy = 'merge',
        merge_update_columns = ['worker_wid', 'worker_id', 'address_id', 'user_id', 'universal_id', 'employee_id', 'contingent_worker_id', 'communication_usage_behavior_id', 'formatted_address', 'address_format_type', 'defaulted_business_site_address_ind', 'deleted_ind', 'do_not_replace_all_ind', 'effective_date', 'last_modified_date', 'municipality', 'country_region_descriptor', 'postal_code', 'number_of_days', 'municipality_local', 'address_line_1', 'address_line_2', 'public_ind', 'is_primary_ind', 'communication_usage_type_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with worker_addr as (
    select distinct
        get_workers_personal_data.worker_worker_reference_wid as worker_wid,
        get_workers_personal_data.worker_worker_data_worker_id as worker_id,
        coalesce(get_workers_personal_data.worker_data_personal_data_contact_data_address_data_address_id, '-2') as address_id,
        get_workers_personal_data.worker_worker_data_user_id as user_id,
        get_workers_personal_data.worker_worker_data_universal_id as universal_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_employee_id as int) as varchar(50)) as employee_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_contingent_worker_id as int) as varchar(50)) as contingent_worker_id,
        coalesce(get_workers_personal_data.contact_data_address_data_usage_data_use_for_tenanted_reference_communication_usage_behavior_tenanted_id, '0') as communication_usage_behavior_id,
        get_workers_personal_data.personal_data_contact_data_address_data_formatted_address as formatted_address,
        coalesce(get_workers_personal_data.personal_data_contact_data_address_data_address_format_type, 'N/A') as address_format_type,
        get_workers_personal_data.personal_data_contact_data_address_data_defaulted_business_site_address::int as defaulted_business_site_address_ind,
        -2 as deleted_ind,
        -2 as do_not_replace_all_ind,
        to_timestamp(get_workers_personal_data.personal_data_contact_data_address_data_effective_date, 'yyyy-mm-dd') as effective_date,
        to_timestamp(replace(substr(get_workers_personal_data.worker_data_personal_data_contact_data_address_data_last_modified, 1, 23), 'T', ' '), 'yyyy-mm-dd hh24:mi:ss.us') - cast(strright(get_workers_personal_data.worker_data_personal_data_contact_data_address_data_last_modified, 4) as time) as last_modified_date,
        get_workers_personal_data.worker_data_personal_data_contact_data_address_data_municipality as municipality,
        get_workers_personal_data.worker_data_personal_data_contact_data_address_data_country_region_descriptor as country_region_descriptor,
        get_workers_personal_data.worker_data_personal_data_contact_data_address_data_postal_code as postal_code,
        cast(get_workers_personal_data.worker_data_personal_data_contact_data_address_data_number_of_days as numeric(30, 2)) as number_of_days,
        get_workers_personal_data.worker_data_personal_data_contact_data_address_data_municipality_local as municipality_local,
        get_workers_personal_data.worker_data_personal_data_contact_data_address_data_address_line_1 as address_line_1,
        get_workers_personal_data.worker_data_personal_data_contact_data_address_data_address_line_2 as address_line_2,
        cast(get_workers_personal_data.personal_data_contact_data_address_data_usage_data_public as int) as public_ind,
        cast(get_workers_personal_data.address_data_usage_data_type_data_primary as int) as is_primary_ind,
        coalesce(get_workers_personal_data.address_data_usage_data_type_data_type_reference_communication_usage_type_id, '-2') as communication_usage_type_id,
        cast({{
            dbt_utils.surrogate_key([
                'worker_wid',
                'worker_id',
                'address_id',
                'user_id',
                'universal_id',
                'employee_id',
                'contingent_worker_id',
                'communication_usage_behavior_id',
                'formatted_address',
                'address_format_type',
                'defaulted_business_site_address_ind',
                'deleted_ind',
                'do_not_replace_all_ind',
                'effective_date',
                'last_modified_date',
                'municipality',
                'country_region_descriptor',
                'postal_code',
                'number_of_days',
                'municipality_local',
                'address_line_1',
                'address_line_2',
                'public_ind',
                'is_primary_ind',
                'communication_usage_type_id'
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
    address_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    communication_usage_behavior_id,
    formatted_address,
    address_format_type,
    defaulted_business_site_address_ind,
    deleted_ind,
    do_not_replace_all_ind,
    effective_date,
    last_modified_date,
    municipality,
    country_region_descriptor,
    postal_code,
    number_of_days,
    municipality_local,
    address_line_1,
    address_line_2,
    public_ind,
    is_primary_ind,
    communication_usage_type_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_addr
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                worker_wid = worker_addr.worker_wid
                and address_id = worker_addr.address_id
                and communication_usage_behavior_id = worker_addr.communication_usage_behavior_id
                and communication_usage_type_id = worker_addr.communication_usage_type_id
                and effective_date = worker_addr.effective_date
        )
    {%- endif %}