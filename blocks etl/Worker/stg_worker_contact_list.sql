{{ config(meta = {
    'critical': true
}) }}

with
    usage_types as (
        select distinct
            worker_email.communication_usage_type_id as usage_type
        from
            {{ source('workday_ods', 'worker_email') }} as worker_email
    ),

    get_email as (
        select
            worker_email.worker_wid,
            worker_email.communication_usage_type_id as usage_type,
            worker_email.public_ind as email_public_ind,
            lower(worker_email.email_address) as email_address
        from
            {{ source('workday_ods', 'worker_email') }} as worker_email
        where
    worker_email.primary_ind = 1
    ),

    get_phone as (
        select
            worker_phone.worker_wid,
            worker_phone.public_ind as phone_public_ind,
            worker_phone.communication_usage_type_id as usage_type,
            case
                when
                    lower(worker_phone.phone_device_type_id) like 'mobile%' 
                    or lower(worker_phone.phone_device_type_id) like '%9m' then 'mobile'
                when lower(worker_phone.phone_device_type_id) like '%9l' then 'landline'
                else lower(worker_phone.phone_device_type_id)
                end as phone_device_type,
            case
                when
                    length(worker_phone.phone_number) != 10 then worker_phone.phone_number
                else regexp_replace(worker_phone.phone_number, '(\d{3})(\d{3})(\d{4})', '(\1) \2-\3')
            end as phone_number,
            case
                --sometimes a phone # is entered?
                when
                    lower(worker_phone.phone_extension) = 'n/a' or length(worker_phone.phone_extension) >= 10
                    then null
                else regexp_replace(worker_phone.phone_extension, 'x', '', 'i') --remove leading x
                end as phone_extension
        from
            {{ source('workday_ods', 'worker_phone') }} as worker_phone
        where
            worker_phone.primary_phone_ind = 1
    ),

    get_address as (
       select
            worker_address.worker_wid,
            worker_address.communication_usage_type_id as usage_type,
            worker_address.public_ind as address_public_ind,
            worker_address.formatted_address as formatted_address,
            worker_address.address_line_1,
            worker_address.address_line_2,
            worker_address.municipality as city,
            worker_address.country_region_descriptor as state,
            worker_address.postal_code as zip,
            --worker_address.last_modified_date, worker_address.communication_usage_behavior_id, worker_address.defaulted_business_site_address_ind,
            dense_rank() over (
                    partition by worker_address.worker_wid, worker_address.communication_usage_type_id
                    order by
                        worker_address.last_modified_date desc,
                        worker_address.communication_usage_behavior_id,
                worker_address.defaulted_business_site_address_ind desc
                ) as priority_seq_num
        from
            {{ source('workday_ods', 'worker_address') }} as worker_address
        where
            worker_address.is_primary_ind = 1
    )

select
    {{
        dbt_utils.surrogate_key([
            'worker.worker_wid',
            'usage_types.usage_type'
        ])
    }} as worker_contact_id,
    worker.legal_reporting_name,
    worker.preferred_reporting_name,
    worker.ad_login,
    worker.active_ind,
    lower(usage_types.usage_type) as usage_type,
    --email
    get_email.email_public_ind,
    get_email.email_address,
    --phone
    get_phone.phone_public_ind,
    get_phone.phone_device_type,
    get_phone.phone_number 
        || case when get_phone.phone_extension is null then '' else ' ext.' || get_phone.phone_extension end
        as full_phone_number,
    --address
    get_address.address_public_ind,
    get_address.formatted_address,
    get_address.address_line_1,
    get_address.address_line_2,
    get_address.city,
    get_address.state,
    get_address.zip,
    --keys
    worker.worker_id,
    worker.worker_wid,
    worker.manager_worker_wid
from
    {{ ref('worker') }} as worker
    cross join usage_types
    left join get_phone
        on get_phone.worker_wid = worker.worker_wid 
        and get_phone.usage_type = usage_types.usage_type
    left join get_email on
        get_email.worker_wid = worker.worker_wid 
        and get_email.usage_type = usage_types.usage_type
    left join get_address 
        on get_address.worker_wid = worker.worker_wid 
        and get_address.usage_type = usage_types.usage_type 
        and get_address.priority_seq_num = 1
where
    coalesce(get_email.email_address, get_phone.phone_number, get_address.address_line_1) is not null
