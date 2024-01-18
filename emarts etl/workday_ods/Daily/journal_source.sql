{{ config(
    materialized = 'incremental',
    unique_key = 'journal_source_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['journal_source_wid','journal_source_id','journal_source_name','source_for_accounting_journal_ind','source_for_ad_hoc_bank_transaction_ind','enable_suspense_processing_for_web_service_ind','suspense_threshold_percent','source_for_workday_operational_journal_ind', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    journal_source_reference_wid as journal_source_wid,
    journal_source_reference_journal_source_id as journal_source_id,
    journal_source_data_journal_source_name as journal_source_name,
    coalesce(cast(journal_source_data_source_for_accounting_journal as int), -2) as source_for_accounting_journal_ind,
    coalesce(cast(journal_source_data_source_for_ad_hoc_bank_transaction as int), -2) as source_for_ad_hoc_bank_transaction_ind,
    coalesce(cast(journal_source_data_enable_suspense_processing_for_web_service as int), -2) as enable_suspense_processing_for_web_service_ind,
    journal_source_data_suspense_threshold_percent as suspense_threshold_percent,
    coalesce(cast(journal_source_data_source_for_workday_operational_journal as int), -2) as source_for_workday_operational_journal_ind,
    cast({{
        dbt_utils.surrogate_key([
            'journal_source_wid',
            'journal_source_id',
            'journal_source_name',
            'source_for_accounting_journal_ind',
            'source_for_ad_hoc_bank_transaction_ind',
            'enable_suspense_processing_for_web_service_ind',
            'suspense_threshold_percent',
            'source_for_workday_operational_journal_ind'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_journal_sources')}} as get_journal_sources
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                journal_source_wid = get_journal_sources.journal_source_reference_wid
        )
    {%- endif %}