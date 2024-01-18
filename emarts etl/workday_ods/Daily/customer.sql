{{ config(
    materialized = 'incremental',
    unique_key = 'customer_wid',
    incremental_strategy = 'merge',
    merge_update_columns = [
        'customer_wid',
        'customer_id',
        'customer_name',
        'worktag_only_ind',
        'duns_number',
        'credit_limit',
        'hierarchy_credit_limit',
        'credit_verification_date',
        'commercial_credit_score',
        'commercial_credit_score_date',
        'commercial_credit_score_note',
        'composite_risk_score',
        'composite_risk_date',
        'composite_risk_note',
        'customer_satisfaction_score',
        'customer_satisfaction_date',
        'customer_satisfaction_note',
        'business_entity_name',
        'external_entity_id',
        'always_separate_payments_ind',
        'mandate_required_ind',
        'followup_date',
        'md5', 'upd_dt', 'upd_by'
    ]
) }}

select distinct  -- The source table has "duplicates" on just these rows. Source worktags differ.
    customer_customer_reference_wid as customer_wid,
    customer_customer_data_customer_id as customer_id,
    customer_customer_data_customer_name as customer_name,
    0 as worktag_only_ind,
    null as duns_number,
    customer_customer_data_credit_limit::numeric(30,2) as credit_limit,
    customer_customer_data_hierarchy_credit_limit::numeric(30,2) as hierarchy_credit_limit,
    null as credit_verification_date,
    customer_customer_data_commercial_credit_score::numeric(30,2) as commercial_credit_score,
    null as commercial_credit_score_date,
    null as commercial_credit_score_note,
    customer_customer_data_composite_risk_score::numeric(30,2) as composite_risk_score,
    null as composite_risk_date,
    null as composite_risk_note,
    customer_customer_data_customer_satisfaction_score::numeric(30,2) as customer_satisfaction_score,
    null as customer_satisfaction_date,
    null as customer_satisfaction_note,
    customer_customer_data_business_entity_data_business_entity_name as business_entity_name,
    null as external_entity_id,
    coalesce(cast(customer_customer_data_always_separate_payments as int), -2)
        as always_separate_payments_ind,
    coalesce(cast(customer_customer_data_mandate_required as int), -2) as mandate_required_ind,
    null as followup_date,
    cast({{
        dbt_utils.surrogate_key(['customer_wid', 'customer_id', 'customer_name', 'credit_limit',
        'hierarchy_credit_limit', 'commercial_credit_score', 'composite_risk_score',
        'customer_satisfaction_score', 'business_entity_name', 'always_separate_payments_ind',
        'mandate_required_ind'])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from {{ source('workday_ods', 'get_customers') }}
where 1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                customer_wid = get_customers.customer_customer_reference_wid
        )
    {%- endif %}
