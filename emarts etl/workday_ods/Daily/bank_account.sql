{{ config(
    materialized = 'incremental',
    unique_key = 'bank_account_wid',
    incremental_strategy = 'merge',
    merge_update_columns = [
        'bank_account_wid',
        'bank_account_id',
        'account_name',
        'bank_account_open_date',
        'account_closed_ind',
        'bank_account_close_date',
        'routing_transit_or_institution_number',
        'financial_account_number',
        'bank_identifier_code',
        'iban',
        'branch_name',
        'check_digit',
        'bank_account_name',
        'roll_number',
        'fraction',
        'formatted_micr',
        'target_balance',
        'use_branch_address_ind',
        'advanced_mode_ind',
        'batch_electronic_payments_ind',
        'batch_electronic_customer_payment_deposits_ind',
        'submit_reconciled_statements_automatically_ind',
        'used_by_cash_ind',
        'used_by_customer_payments_ind',
        'used_by_customer_refunds_ind',
        'used_by_expense_payments_ind',
        'used_by_payroll_on_cycle_ind',
        'used_by_supplier_payments_ind',
        'used_by_intercompany_payments_ind',
        'used_by_ad_hoc_payments_ind',
        'used_by_payroll_off_cycle_ind',
        'used_by_bank_account_transfers_for_settlement_ind',
        'used_by_prenote_payments_ind',
        'used_by_procurement_card_payments_ind',
        'used_by_tax_payments_ind',
        'used_by_cash_advances_ind',
        'used_by_expense_credit_card_payments_ind',
        'used_by_student_refund_ind',
        'used_by_student_payment_ind',
        'last_check_number_used',
        'enable_positive_pay_ind',
        'outsourced_ind',
        'allow_additional_usage_ind',
        'default_bank_statement_beginning_balance_ind',
        'financial_institution_wid',
        'financial_institution_id',
        'company_id',
        'md5',
        'upd_dt',
        'upd_by'
    ]
) }}

select distinct  -- duplicates exist in the source table
    bank_account_bank_account_reference_wid as bank_account_wid,
    bank_account_bank_account_data_bank_account_id as bank_account_id,
    cast(replace(bank_account_bank_account_data_account_name, chr(160), ' ') as varchar(100))
        as account_name,  -- handle non-ascii character
    cast(bank_account_bank_account_data_bank_account_open_date as date) as bank_account_open_date,
    coalesce(cast(bank_account_bank_account_data_account_closed as int), -2) as account_closed_ind,
    null as bank_account_close_date,
    bank_account_bank_account_data_routing_transit_or_institution_number
        as routing_transit_or_institution_number,
    bank_account_bank_account_data_financial_account_number as financial_account_number,
    bank_account_bank_account_data_bank_identifier_code as bank_identifier_code,
    null as iban,
    null as branch_name,
    null as check_digit,
    null as bank_account_name,
    null as roll_number,
    bank_account_bank_account_data_fraction as fraction,
    bank_account_bank_account_data_formatted_micr as formatted_micr,
    cast(bank_account_bank_account_data_target_balance as numeric(30,2)) as target_balance,
    coalesce(cast(bank_account_bank_account_data_use_branch_address as int), -2)
        as use_branch_address_ind,
    coalesce(cast(bank_account_bank_account_data_advanced_mode as int), -2) as advanced_mode_ind,
    coalesce(cast(bank_account_bank_account_data_batch_electronic_payments as int), -2)
        as batch_electronic_payments_ind,
    coalesce(
        cast(bank_account_bank_account_data_batch_electronic_customer_payment_deposits as int),
        -2)
        as batch_electronic_customer_payment_deposits_ind,
    coalesce(
        cast(bank_account_bank_account_data_submit_reconciled_statements_automatically as int),
        -2)
        as submit_reconciled_statements_automatically_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_cash as int), -2) as used_by_cash_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_customer_payments as int), -2)
        as used_by_customer_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_customer_refunds as int), -2)
        as used_by_customer_refunds_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_expense_payments as int), -2)
        as used_by_expense_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_payroll_on_cycle as int), -2)
        as used_by_payroll_on_cycle_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_supplier_payments as int), -2)
        as used_by_supplier_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_intercompany_payments as int), -2)
        as used_by_intercompany_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_ad_hoc_payments as int), -2)
        as used_by_ad_hoc_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_payroll_off_cycle as int), -2)
        as used_by_payroll_off_cycle_ind,
    coalesce(
        cast(bank_account_bank_account_data_used_by_bank_account_transfers_for_settlement as int),
        -2)
        as used_by_bank_account_transfers_for_settlement_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_prenote_payments as int), -2)
        as used_by_prenote_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_procurement_card_payments as int), -2)
        as used_by_procurement_card_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_tax_payments as int), -2)
        as used_by_tax_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_cash_advances as int), -2)
        as used_by_cash_advances_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_expense_credit_card_payments as int), -2)
        as used_by_expense_credit_card_payments_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_student_refund as int), -2)
        as used_by_student_refund_ind,
    coalesce(cast(bank_account_bank_account_data_used_by_student_refund as int), -2)
        as used_by_student_payment_ind,
    cast(bank_account_bank_account_data_last_check_number_used as numeric(30,2))
        as last_check_number_used,
    coalesce(cast(bank_account_bank_account_data_enable_positive_pay as int), -2)
        as enable_positive_pay_ind,
    coalesce(cast(bank_account_bank_account_data_outsourced as int), -2) as outsourced_ind,
    coalesce(cast(bank_account_bank_account_data_allow_additional_usage as int), -2)
        as allow_additional_usage_ind,
    coalesce(
        cast(bank_account_bank_account_data_default_bank_statement_beginning_balance as int),
        -2)
        as default_bank_statement_beginning_balance_ind,
    bank_account_data_financial_institution_reference_wid as financial_institution_wid,
    bank_account_data_financial_institution_reference_financial_institution_reference_id
        as financial_institution_id,
    bank_account_data_financial_party_reference_company_reference_id as company_id,
    cast({{
        dbt_utils.surrogate_key([
            'bank_account_wid',
            'bank_account_id',
            'account_name',
            'bank_account_open_date',
            'account_closed_ind',
            'bank_account_close_date',
            'routing_transit_or_institution_number',
            'financial_account_number',
            'bank_identifier_code',
            'iban',
            'branch_name',
            'check_digit',
            'bank_account_name',
            'roll_number',
            'fraction',
            'formatted_micr',
            'target_balance',
            'use_branch_address_ind',
            'advanced_mode_ind',
            'batch_electronic_payments_ind',
            'batch_electronic_customer_payment_deposits_ind',
            'submit_reconciled_statements_automatically_ind',
            'used_by_cash_ind',
            'used_by_customer_payments_ind',
            'used_by_customer_refunds_ind',
            'used_by_expense_payments_ind',
            'used_by_payroll_on_cycle_ind',
            'used_by_supplier_payments_ind',
            'used_by_intercompany_payments_ind',
            'used_by_ad_hoc_payments_ind',
            'used_by_payroll_off_cycle_ind',
            'used_by_bank_account_transfers_for_settlement_ind',
            'used_by_prenote_payments_ind',
            'used_by_procurement_card_payments_ind',
            'used_by_tax_payments_ind',
            'used_by_cash_advances_ind',
            'used_by_expense_credit_card_payments_ind',
            'used_by_student_refund_ind',
            'used_by_student_payment_ind',
            'last_check_number_used',
            'enable_positive_pay_ind',
            'outsourced_ind',
            'allow_additional_usage_ind',
            'default_bank_statement_beginning_balance_ind',
            'financial_institution_wid',
            'financial_institution_id',
            'company_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_bank_accounts')}}
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                bank_account_wid = get_bank_accounts.bank_account_bank_account_reference_wid
        )
    {%- endif %}
