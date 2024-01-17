{{
  config(
    materialized = 'incremental',
    unique_key = 'journal_source_wid',
    incremental_strategy = 'merge',
    merge_update_columns = [ 'journal_source_id', 'journal_source_name', 'source_for_accounting_journal_ind', 'source_for_ad_hoc_bank_transaction_ind', 'enable_suspense_processing_for_web_service_ind', 'suspense_threshold_percent', 'source_for_workday_operational_journal_ind', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'journal_source'), except=['md5', 'create_dt', 'create_by', 'upd_dt', 'upd_by']) %}
with journal_source
as (
select
    {{
        dbt_utils.surrogate_key([
            'journal_source.journal_source_wid'
        ])
    }} as journal_source_key,
    journal_source.journal_source_wid,
    journal_source.journal_source_id,
    journal_source.journal_source_name,
    journal_source.source_for_accounting_journal_ind,
    journal_source.source_for_ad_hoc_bank_transaction_ind,
    journal_source.enable_suspense_processing_for_web_service_ind,
    journal_source.suspense_threshold_percent,
    journal_source.source_for_workday_operational_journal_ind,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    journal_source.create_by || '~' || journal_source.journal_source_id as integration_id,
    current_timestamp as create_date,
    journal_source.create_by,
    current_timestamp as update_date,
    journal_source.upd_by as update_by
from
    {{source('workday_ods', 'journal_source')}} as journal_source
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    0,
    0,
    0,
    0,
    0,
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP,
    'DEFAULT'
)
select
    journal_source.journal_source_key,
    journal_source.journal_source_wid,
    journal_source.journal_source_id,
    journal_source.journal_source_name,
    journal_source.source_for_accounting_journal_ind,
    journal_source.source_for_ad_hoc_bank_transaction_ind,
    journal_source.enable_suspense_processing_for_web_service_ind,
    journal_source.suspense_threshold_percent,
    journal_source.source_for_workday_operational_journal_ind,
    journal_source.hash_value,
    journal_source.integration_id,
    journal_source.create_date,
    journal_source.create_by,
    journal_source.update_date,
    journal_source.update_by
from
    journal_source
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where journal_source_wid = journal_source.journal_source_wid)
{%- endif %}
