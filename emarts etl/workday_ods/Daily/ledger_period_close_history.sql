{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge'
    )
}}
with ledger_period_close_history_stg as (
    select distinct
        company_wid,
        company_id,
        fiscal_period_wid,
        fiscal_period_start_date,
        fiscal_period_end_date,
        fiscal_period_descriptor,
        ledger_type_wid,
        ledger_type_id,
        ledger_type_descriptor,
        ledger_period_status_wid,
        ledger_period_status_id,
        ledger_period_status_descriptor,
        ledger_period_descriptor,
        book_code_descriptor,
        last_updated_by as closed_by,
        last_update_date as closed_date,
        last_update_date_utc_offset as closed_date_utc_offset
    from
        {{ref('ledger_period_close_status')}} as ledger_period_close_status
    where
        lower(ledger_period_status_id) = 'closed'
)
select
    company_wid,
    company_id,
    fiscal_period_wid,
    fiscal_period_start_date,
    fiscal_period_end_date,
    fiscal_period_descriptor,
    ledger_type_wid,
    ledger_type_id,
    ledger_type_descriptor,
    ledger_period_status_wid,
    ledger_period_status_id,
    ledger_period_status_descriptor,
    ledger_period_descriptor,
    book_code_descriptor,
    closed_by,
    closed_date,
    closed_date_utc_offset,
    cast({{
        dbt_utils.surrogate_key([
            'company_wid',
            'company_id',
            'fiscal_period_wid',
            'fiscal_period_start_date',
            'fiscal_period_end_date',
            'fiscal_period_descriptor',
            'ledger_type_wid',
            'ledger_type_id',
            'ledger_type_descriptor',
            'ledger_period_status_wid',
            'ledger_period_status_id',
            'ledger_period_status_descriptor',
            'ledger_period_descriptor',
            'book_code_descriptor',
            'closed_by',
            'closed_date',
            'closed_date_utc_offset'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    ledger_period_close_history_stg
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                company_wid <> ledger_period_close_history_stg.company_wid
                and fiscal_period_wid <> ledger_period_close_history_stg.fiscal_period_wid
                and closed_date <> ledger_period_close_history_stg.closed_date
                and closed_date_utc_offset <> ledger_period_close_history_stg.closed_date_utc_offset
        )
    {%- endif %}
