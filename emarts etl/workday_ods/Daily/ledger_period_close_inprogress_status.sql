{{
    config(
        materialized = 'incremental',
        unique_key = ['company_wid','fiscal_period_wid','period_close_activity_group_wid'],
        incremental_strategy = 'merge',
        merge_update_columns = ['company_wid','company_id','fiscal_period_wid','fiscal_period_start_date','fiscal_period_end_date','fiscal_period_descriptor','ledger_type_wid','ledger_type_id','ledger_type_descriptor','ledger_period_status_wid','ledger_period_status_id','ledger_period_status_descriptor','ledger_period_descriptor','book_code_descriptor','period_close_activity_group_wid','period_close_activity_group_id','period_close_activity_group_descriptor','last_updated_by','last_update_date','last_update_date_utc_offset', 'md5', 'upd_dt', 'upd_by']
    )
}}
with ledger_period_close_inprogress as (
    select distinct
        company_wid,
        company_reference_id as company_id,
        fiscal_period_wid,
        to_date(substring(fiscal_period_start_date, 1, 10),'YYYY-MM-DD') as fiscal_period_start_date,
        to_date(substring(fiscal_period_end_date, 1, 10),'YYYY-MM-DD') as fiscal_period_end_date,
        fiscal_period as fiscal_period_descriptor,
        ledger_type_wid,
        ledger_type_id,
        ledger_type as ledger_type_descriptor,
        ledger_period_status_wid,
        ledger_period_status_id,
        ledger_period_status as ledger_period_status_descriptor,
        ledger_period as ledger_period_descriptor,
        book_code as book_code_descriptor,
        period_close_activity_group_wid,
        period_close_activity_group_id,
        period_close_activity_group__tenanted_ as period_close_activity_group_descriptor,
        last_updated_by,
        to_timestamp(substring(last_update_date, 1, 10) || ' ' || substring(last_update_date, 12, 8) , 'yyyy-mm-dd hh24:mi:ss') as last_update_date,
        substring(last_update_date, 21, 5) as last_update_date_utc_offset
    from
        {{source('workday_ods', 'workday_ledger_period_close_status')}} as workday_ledger_period_close_status
    where
        lower(ledger_period_status_id) = 'close_in_progress'
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
    period_close_activity_group_wid,
    period_close_activity_group_id,
    period_close_activity_group_descriptor,
    last_updated_by,
    last_update_date,
    last_update_date_utc_offset,
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
            'period_close_activity_group_wid',
            'period_close_activity_group_id',
            'period_close_activity_group_descriptor',
            'last_updated_by',
            'last_update_date',
            'last_update_date_utc_offset'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    ledger_period_close_inprogress
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                company_wid = ledger_period_close_inprogress.company_wid
                and fiscal_period_wid = ledger_period_close_inprogress.fiscal_period_wid
                and period_close_activity_group_wid = ledger_period_close_inprogress.period_close_activity_group_wid
        )
    {%- endif %}