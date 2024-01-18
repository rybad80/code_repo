{{ config(
    materialized = 'incremental',
    unique_key = 'award_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['award_wid', 'award_id', 'submit_ind', 'locked_in_workday_ind', 'award_number', 'sponsor_award_reference_number', 'award_name', 'award_description', 'award_effective_date', 'award_signed_date', 'institutional_id', 'sub_award_ind', 'letter_of_credit_document_id', 'award_sequence_billing_active_reference_ind', 'award_billing_sequence_number_format_syntax_reference', 'current_award_billing_sequence_number_used_reference', 'sponsor_direct_cost_amount', 'sponsor_facilities_and_administration_amount', 'cost_share_total_amount', 'authorized_amount', 'billing_limit_override', 'cost_share_required_by_sponsor_ind', 'anticipated_sponsor_direct_cost_amount', 'anticipated_facilities_and_administration_amount', 'federal_award_id_number', 'cfda_number', 'cfda_description', 'proposal_id', 'proposal_version', 'award_notes', 'billing_notes', 'company_id', 'contract_owner_employee_id', 'payment_terms_id', 'payment_type_id', 'prime_sponsor_id', 'sponsor_id', 'sponsor_award_type_id', 'award_lifecycle_status_id', 'award_schedule_reference_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    award_award_reference_wid as award_wid,
    award_award_reference_award_reference_id as award_id,
    -2 as submit_ind,
    -2 as locked_in_workday_ind,
    award_award_data_award_number as award_number,
    award_award_data_sponsor_award_reference_number as sponsor_award_reference_number,
    award_award_data_award_name as award_name,
    substr(award_award_data_award_description, 1, 150) as award_description,
    to_timestamp(award_award_data_award_effective_date, 'yyyy-mm-dd') as award_effective_date,
    to_timestamp(award_award_data_award_signed_date, 'yyyy-mm-dd') as award_signed_date,
    award_award_data_institutional_id as institutional_id,
    coalesce(cast(award_award_data_sub_award as int), -2) as sub_award_ind,
    award_award_data_letter_of_credit_document_id as letter_of_credit_document_id,
    award_award_data_award_sequence_billing_active_reference as award_sequence_billing_active_reference_ind,
    null as award_billing_sequence_number_format_syntax_reference,
    award_award_data_current_award_billing_sequence_number_used_reference as current_award_billing_sequence_number_used_reference,
    award_award_data_sponsor_direct_cost_amount as sponsor_direct_cost_amount,
    award_award_data_sponsor_facilities_and_administration_amount as sponsor_facilities_and_administration_amount,
    award_award_data_cost_share_total_amount as cost_share_total_amount,
    award_award_data_authorized_amount as authorized_amount,
    award_award_data_billing_limit_override as billing_limit_override,
    coalesce(cast(award_award_data_cost_share_required_by_sponsor as int), -2) as cost_share_required_by_sponsor_ind,
    award_award_data_anticipated_sponsor_direct_cost_amount as anticipated_sponsor_direct_cost_amount,
    award_award_data_anticipated_facilities_and_administration_amount as anticipated_facilities_and_administration_amount,
    award_award_data_federal_award_id_number as federal_award_id_number,
    award_data_cfda_number_reference_cfda_number as cfda_number,
    null as cfda_description,
    substr(award_award_data_proposal_id, 1, 50) as proposal_id,
    substr(award_award_data_proposal_version, 1, 50) as proposal_version,
    award_award_data_award_notes as award_notes,
    null as billing_notes,
    award_data_company_reference_company_reference_id as company_id,
    award_data_award_contract_owner_reference_employee_id as contract_owner_employee_id,
    award_data_payment_terms_reference_payment_terms_id as payment_terms_id,
    award_data_payment_type_reference_payment_type_id as payment_type_id,
    award_data_prime_sponsor_reference_sponsor_id as prime_sponsor_id,
    award_data_sponsor_reference_sponsor_reference_id as sponsor_id,
    award_data_award_type_reference_sponsor_award_type_id as sponsor_award_type_id,
    award_data_award_lifecycle_status_reference_award_lifecycle_status_id as award_lifecycle_status_id,
    award_data_award_schedule_reference_award_schedule_reference_id as award_schedule_reference_id,
    cast({{
        dbt_utils.surrogate_key([
            'award_wid',
            'award_id',
            'submit_ind',
            'locked_in_workday_ind',
            'award_number',
            'sponsor_award_reference_number',
            'award_name',
            'award_description',
            'award_effective_date',
            'award_signed_date',
            'institutional_id',
            'sub_award_ind',
            'letter_of_credit_document_id',
            'award_sequence_billing_active_reference_ind',
            'award_billing_sequence_number_format_syntax_reference',
            'current_award_billing_sequence_number_used_reference',
            'sponsor_direct_cost_amount',
            'sponsor_facilities_and_administration_amount',
            'cost_share_total_amount',
            'authorized_amount',
            'billing_limit_override',
            'cost_share_required_by_sponsor_ind',
            'anticipated_sponsor_direct_cost_amount',
            'anticipated_facilities_and_administration_amount',
            'federal_award_id_number',
            'cfda_number',
            'cfda_description',
            'proposal_id',
            'proposal_version',
            'award_notes',
            'billing_notes',
            'company_id',
            'contract_owner_employee_id',
            'payment_terms_id',
            'payment_type_id',
            'prime_sponsor_id',
            'sponsor_id',
            'sponsor_award_type_id',
            'award_lifecycle_status_id',
            'award_schedule_reference_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_awards_data')}} as get_awards_data
where
    1 = 1
    and get_awards_data.award_award_reference_award_reference_id is not null
    and award_award_reference_wid <> '8b24be9f320f0128ecc1ad962910e60b'
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                award_wid = get_awards_data.award_award_reference_wid
        )
    {%- endif %}
