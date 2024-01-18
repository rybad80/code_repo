select distinct  -- the source table "duplicates" much of this data over the worktags
    payroll_deduction_recipient_payroll_deduction_recipient_reference_wid as deduction_recipient_wid, -- primary key
    payroll_deduction_recipient_payroll_deduction_recipient_reference_deduction_recipient_id
        as deduction_recipient_id,
    payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_address_id
        as address_id,
    business_entity_data_contact_data_address_data_formatted_address
        as formatted_address,
    business_entity_data_contact_data_address_data_address_format_type
        as address_format_type,
    coalesce(cast(business_entity_data_contact_data_address_data_defaulted_business_site_address as int), -2)
        as defaulted_business_site_address_ind,
    -2 as delete_ind,
    -2 as do_not_replace_all_ind,
    to_timestamp(business_entity_data_contact_data_address_data_effective_date, 'yyyy-mm-dd')
        as effective_date,
    --to_timestamp((substring(payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_last_modified, 1, 10) || ' ' || substring(payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_last_modified, 12, 8)), 'yyyy-mm-dd hh:mi:ss')
--        as last_modified_date,
    to_timestamp(replace(substr(payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_last_modified,1,23),'T',' '),'yyyy-mm-dd hh24:mi:ss.us') - cast(strright(payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_last_modified,5) as time) as last_modified_date,
    payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_municipality
        as municipality,
    payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_country_region_descriptor
        as country_region_descriptor,
    payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_postal_code
        as postal_code,
    payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_number_of_days
        as number_of_days,
    null as municipality_local,
    coalesce(cast(contact_data_address_data_usage_data_type_data_primary as int), -2) as primary_ind,
    coalesce(cast(business_entity_data_contact_data_address_data_usage_data_public as int), -2) as public_ind,
    cast({{
        dbt_utils.surrogate_key([
            'deduction_recipient_wid',
            'deduction_recipient_id',
            'address_id',
            'formatted_address',
            'address_format_type',
            'defaulted_business_site_address_ind',
            'delete_ind',
            'do_not_replace_all_ind',
            'effective_date',
            'last_modified_date',
            'municipality',
            'country_region_descriptor',
            'postal_code',
            'number_of_days',
            'municipality_local',
            'primary_ind',
            'public_ind'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{ source('workday_ods', 'get_payroll_deduction_recipients') }} as get_payroll_deduction_recipients
where
    1=1
    and payroll_deduction_recipient_data_business_entity_data_contact_data_address_data_address_id is not null
