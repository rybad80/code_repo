{{ config(materialized='table', dist='coverage_id') }}

select
    arpb_transactions.tx_id,
    arpb_transactions.coverage_id,
    arpb_transactions.payor_id
from
    {{source('clarity_ods', 'arpb_transactions')}} as arpb_transactions
