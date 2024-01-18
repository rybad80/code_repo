{{ config(
    materialized = 'incremental',
    unique_key = 'ledger_account_summary_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['ledger_account_summary_wid','ledger_account_summary_id','ledger_account_summary_name','ledger_account_summary_parent_id', 'ledger_account_summary_reference_parent_type', 'effective_date', 'md5', 'upd_dt', 'upd_by']
) }}
with distinct_included as (
    select distinct
        ledger_account_summary_data_ledger_account_summary_reference_master as included
    from {{source('workday_ods', 'get_ledger_account_summaries')}} as get_ledger_account_summaries
),
hierarchy as (
    select distinct
        ledger_account_summary_ledger_account_summary_data_ledger_account_summary_id as superior,
        ledger_account_summary_data_ledger_account_summary_reference_master as included
    from
        {{source('workday_ods', 'get_ledger_account_summaries')}} as get_ledger_account_summaries
    left join
        distinct_included
            on get_ledger_account_summaries.ledger_account_summary_ledger_account_summary_data_ledger_account_summary_id = distinct_included.included
)
select distinct
    ledger_account_summary_ledger_account_summary_reference_wid as ledger_account_summary_wid,
    ledger_account_summary_ledger_account_summary_data_ledger_account_summary_id as ledger_account_summary_id,
    ledger_account_summary_ledger_account_summary_data_ledger_account_summary_name as ledger_account_summary_name,
    hierarchy.superior as ledger_account_summary_parent_id,
    'Account_Set_ID' as ledger_account_summary_reference_parent_type,
    ledger_account_summary_ledger_account_summary_data_effective_date as effective_date,
    cast({{
        dbt_utils.surrogate_key([
            'ledger_account_summary_wid',
            'ledger_account_summary_id',
            'ledger_account_summary_name',
            'ledger_account_summary_parent_id',
            'ledger_account_summary_reference_parent_type',
            'effective_date'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_ledger_account_summaries')}} as get_ledger_account_summaries
left join
    hierarchy
        on get_ledger_account_summaries.ledger_account_summary_ledger_account_summary_data_ledger_account_summary_id = hierarchy.included
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                ledger_account_summary_wid = get_ledger_account_summaries.ledger_account_summary_ledger_account_summary_reference_wid
        )
    {%- endif %}
