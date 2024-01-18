{{ config(
    materialized = 'incremental',
    unique_key = 'deduction_recipient_wid',
    incremental_strategy = 'merge',
    merge_update_columns = [
	    'deduction_recipient_wid',
	    'deduction_recipient_id',
	    'payroll_alternate_deduction_recipient_name',
	    'payroll_deduction_recipient_name',
	    'business_entity_name',
	    'external_entity_id',
	    'payment_type_id',
        'md5', 'upd_dt', 'upd_by'
    ]
) }}

select distinct  -- the source table "duplicates" much of this data over the worktags
    payroll_deduction_recipient_payroll_deduction_recipient_reference_wid as deduction_recipient_wid, -- primary key
    payroll_deduction_recipient_payroll_deduction_recipient_reference_deduction_recipient_id
        as deduction_recipient_id,
    payroll_deduction_recipient_payroll_deduction_recipient_data_payroll_alternate_deduction_recipient_name
        as payroll_alternate_deduction_recipient_name,
    payroll_deduction_recipient_payroll_deduction_recipient_data_payroll_deduction_recipient_name
        as payroll_deduction_recipient_name,
    payroll_deduction_recipient_payroll_deduction_recipient_data_business_entity_data_business_entity_name
        as business_entity_name,
    null as business_entity_phonetic_name,
    payroll_deduction_recipient_payroll_deduction_recipient_data_business_entity_data_external_entity_id
        as external_entity_id,
    null as business_entity_tax_id,
    payroll_deduction_recipient_payroll_deduction_recipient_data_payment_type_reference_payment_type_id
        as payment_type_id,
    cast({{
        dbt_utils.surrogate_key([
            'deduction_recipient_wid',
            'deduction_recipient_id',
            'payroll_alternate_deduction_recipient_name',
            'payroll_deduction_recipient_name',
            'business_entity_name',
            'external_entity_id',
            'payment_type_id'
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
    {%- if is_incremental() %}
        and md5 not in (
            select
                md5
            from
                {{ this }}
            where
                deduction_recipient_wid = get_payroll_deduction_recipients.payroll_deduction_recipient_payroll_deduction_recipient_reference_wid
        )
    {%- endif %}
