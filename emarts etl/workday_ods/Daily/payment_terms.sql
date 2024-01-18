{{ config(
    materialized = 'incremental',
    unique_key = 'payment_terms_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['payment_terms_wid','payment_terms_id','payment_terms_name','due_days', 'payment_discount_days', 'payment_discount_percent', 'grace_days', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    payment_term_reference_wid as payment_terms_wid,
    payment_term_reference_payment_terms_id as payment_terms_id,
    payment_term_data_payment_terms_name as payment_terms_name,
    null as due_days,
    payment_term_data_payment_discount_days as payment_discount_days,
    payment_term_data_payment_discount_percent as payment_discount_percent,
    payment_term_data_grace_days as grace_days,
    cast({{
        dbt_utils.surrogate_key([
            'payment_terms_wid',
            'payment_terms_id',
            'payment_terms_name',
            'due_days',
            'payment_discount_days',
            'payment_discount_percent',
            'grace_days'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_payment_terms')}} as get_payment_terms
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                payment_terms_wid = get_payment_terms.payment_term_reference_wid
        )
    {%- endif %}
