{{ config(
    materialized = 'incremental',
    unique_key = 'withholding_order_wid',
    incremental_strategy = 'merge',
    merge_update_columns = [
	    'withholding_order_wid',
        'withholding_order_case_id',
        'withholding_order_id',
        'withholding_order_additional_order_number',
        'withholding_order_type_code',
        'order_date',
        'received_date',
        'begin_date',
        'end_date',
        'inactive_order_ind',
        'withholding_order_amount',
        'total_debt_amount',
        'monthly_limit',
        'originating_authority',
        'memo',
        'original_order_ind',
        'amended_order_ind',
        'terminated_order_ind',
        'custodial_party_name',
        'supports_second_family_ind',
        'remittance_id_override',
        'payroll_local_county_authority_fips_code',
        'employee_id',
        'employee_wid',
        'company_id',
        'withholding_order_amount_type_id',
        'frequency_id',
        'payroll_state_authority_tax_code',
        'payroll_federal_authority_tax_code',
        'deduction_recipient_wid',
        'deduction_recipient_id',
        'md5', 'upd_dt', 'upd_by'
    ]
) }}
select distinct  -- the source table "duplicates" much of this data over the worktags
    payroll_involuntary_withholding_order_reference_wid as withholding_order_wid,
    withholding_order_case_reference_withholding_order_case_id as withholding_order_case_id,
    payroll_involuntary_withholding_order_reference_withholding_order_id as withholding_order_id,
    payroll_involuntary_withholding_order_data_withholding_order_additional_order_number as withholding_order_additional_order_number,
    withholding_order_type_reference_withholding_order_type_code as withholding_order_type_code,
    to_timestamp(payroll_involuntary_withholding_order_data_order_date, 'yyyy-mm-dd') as order_date,
    to_timestamp(replace(substr(payroll_involuntary_withholding_order_data_received_date,1,19),'T',' '),'yyyy-mm-dd hh24:mi:ss') - cast(strright(payroll_involuntary_withholding_order_data_received_date,5) as time) as received_date,
    to_timestamp(payroll_involuntary_withholding_order_data_begin_date, 'yyyy-mm-dd') as begin_date,
    to_timestamp(payroll_involuntary_withholding_order_data_end_date, 'yyyy-mm-dd') as end_date,
    coalesce(cast(payroll_involuntary_withholding_order_data_inactive_order as int), -2) as inactive_order_ind,
    payroll_involuntary_withholding_order_data_withholding_order_amount as withholding_order_amount,
    payroll_involuntary_withholding_order_data_total_debt_amount as total_debt_amount,
    payroll_involuntary_withholding_order_data_monthly_limit as monthly_limit,
    null as originating_authority,
    null as memo,
    coalesce(cast(support_order_data_original_order as int), -2) as original_order_ind,
    coalesce(cast(support_order_data_amended_order as int), -2) as amended_order_ind,
    coalesce(cast(support_order_data_termination_order as int), -2) as terminated_order_ind,
    null as custodial_party_name,
    coalesce(cast(support_order_data_supports_second_family as int), -2) as supports_second_family_ind,
    null as remittance_id_override,
    null as payroll_local_county_authority_fips_code,
    employee_reference_employee_id as employee_id,
    employee_reference_wid as employee_wid,
    company_reference_company_reference_id as company_id,
    withholding_order_amount_type_reference_withholding_order_amount_type_id as withholding_order_amount_type_id,
    pay_period_frequency_reference_frequency_id as frequency_id,
    issued_in_reference_payroll_state_authority_tax_code as payroll_state_authority_tax_code,
    issued_in_reference_payroll_federal_authority_tax_code as payroll_federal_authority_tax_code,
    deduction_recipient_reference_wid as deduction_recipient_wid,
    deduction_recipient_reference_deduction_recipient_id as deduction_recipient_id,
    cast({{
        dbt_utils.surrogate_key([
            'withholding_order_wid',
            'withholding_order_case_id',
            'withholding_order_id',
            'withholding_order_additional_order_number',
            'withholding_order_type_code',
            'order_date',
            'received_date',
            'begin_date',
            'end_date',
            'inactive_order_ind',
            'withholding_order_amount',
            'total_debt_amount',
            'monthly_limit',
            'originating_authority',
            'memo',
            'original_order_ind',
            'amended_order_ind',
            'terminated_order_ind',
            'custodial_party_name',
            'supports_second_family_ind',
            'remittance_id_override',
            'payroll_local_county_authority_fips_code',
            'employee_id',
            'employee_wid',
            'company_id',
            'withholding_order_amount_type_id',
            'frequency_id',
            'payroll_state_authority_tax_code',
            'payroll_federal_authority_tax_code',
            'deduction_recipient_wid',
            'deduction_recipient_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{ source('workday_ods', 'get_payroll_involuntary_withholding_orders') }} as get_payroll_involuntary_withholding_orders
where
    1=1
    {%- if is_incremental() %}
        and md5 not in (
            select
                md5
            from
                {{ this }}
            where
                withholding_order_wid = get_payroll_involuntary_withholding_orders.payroll_involuntary_withholding_order_reference_wid
        )
    {%- endif %}
