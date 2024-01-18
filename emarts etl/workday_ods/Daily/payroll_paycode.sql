{{
    config(
        materialized = 'incremental',
        unique_key = ['pay_component_type', 'pay_component_wid'],
        incremental_strategy = 'merge',
        merge_update_columns = ['pay_component_type', 'pay_component_wid', 'pay_component_code', 'pay_component_desc', 'md5', 'upd_dt', 'upd_by']
    )
}}
with earnings as (
    select distinct
        'EARNING' as pay_component_type,
        earning_wid as pay_component_wid,
        earning_reference_id as pay_component_code,
        earning as pay_component_desc
    from
        {{source('workday_ods', 'workday_earning_codes')}} as workday_earning_codes
),
deductions as (
    select distinct
        'DEDUCTION' as pay_component_type,
        deduction_wid as pay_component_wid,
        deduction_reference_id as pay_component_code,
        deduction as pay_component_desc
    from
        {{source('workday_ods', 'workday_deduction_codes')}} as workday_deduction_codes
),
earnings_and_deductions as (
    select * from earnings
    union
    select * from deductions
)
select
    pay_component_type,
    pay_component_wid,
    pay_component_code,
    pay_component_desc,
    cast({{
        dbt_utils.surrogate_key([
            'pay_component_type',
            'pay_component_wid',
            'pay_component_code',
            'pay_component_desc'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    earnings_and_deductions
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                pay_component_type = earnings_and_deductions.pay_component_type
                and pay_component_wid = earnings_and_deductions.pay_component_wid
        )
    {%- endif %}
