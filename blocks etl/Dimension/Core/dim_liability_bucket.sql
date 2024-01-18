-- "Check columns" are the columns that are used in the hash value to check for changes.
-- These are the non-key and non-audit columns. Use single quotes.
{% set check_columns = ['liability_bucket_id', 'liability_bucket_name', 'liabilty_bucket_number', 'payor_key', 'coverage_key', 'benefit_plan_key', 'service_area_key', 'current_balance',
'previous_credits', 'charge_total', 'charge_reversal_total', 'payment_total', 'adjustment_total', 'next_responsible_amount', 'first_claim_date',
'last_claim_date', 'interim_start_date', 'interim_end_date', 'bucket_create_date', 'open_remark_bdc_ind', 'open_denial_bdc_ind',
'open_correspondence_bdc_ind', 'bad_debt_ind', 'external_ar_ind', 'claim_accepted_ind', 'current_auto_write_off_tx_id', 'reversal_auto_write_off_tx_id',
'actual_not_allowed_write_off_amount', 'expected_not_allowed_write_off_amount', 'liability_bucket_type', 'liability_bucket_status', 'interim_liability_bucket_type',
'close_reason', 'write_off_adjustment_status', 'claim_type', 'medicare_claim'] %}
 
{{ config(
    materialized = 'incremental',
    unique_key = 'liability_bucket_key',
    incremental_strategy = 'merge',
    merge_update_columns = check_columns + ['hash_value', 'integration_id', 'update_date',
        'update_source'],
    meta = {
        'critical': true
    }
) }}
 
with unionset as (
    select
        {{
            dbt_utils.surrogate_key([
                'bucket_id',
                "'CLARITY'"
            ])
        }} as liability_bucket_key,
        bucket_id as liability_bucket_id,
        bucket_name as liability_bucket_name,
        bkt_num as liabilty_bucket_number,
        hsp_account_id,
        {{
            dbt_utils.surrogate_key([
                'hsp_account_id',
                "'CLARITY'"
            ])
        }} as hospital_account_key,
        {{
            dbt_utils.surrogate_key([
                'payor_id',
                "'CLARITY'"
            ])
        }} as payor_key,
        {{
            dbt_utils.surrogate_key([
                'coverage_id',
                "'CLARITY'"
            ])
        }} as coverage_key,
        {{
            dbt_utils.surrogate_key([
                'benefit_plan_id',
                "'CLARITY'"
            ])
        }} as benefit_plan_key,
        {{
            dbt_utils.surrogate_key([
                'serv_area_id',
                "'CLARITY'"
            ])
        }} as service_area_key,
        current_balance,
        previous_credits,
        charge_total,
        charge_rev_total as charge_reversal_total,
        payment_total,
        adjustment_total,
        next_resp_amt as next_responsible_amount,
        first_claim_date,
        last_claim_date,
        interim_start_date,
        interim_end_date,
        record_create_date as bucket_create_date,
        case
            when lower('open_rmk_bdc_yn') = 'y'
            then 1
            else 0
        end as open_remark_bdc_ind,
        case
            when lower('open_denial_bdc_yn') = 'y'
            then 1
            else 0
        end as open_denial_bdc_ind,
        case
            when lower('open_cor_bdc_yn') = 'y'
            then 1
            else 0
        end as open_correspondence_bdc_ind,
        case
            when lower('bad_debt_flag_yn') = 'y'
            then 1
            else 0
        end as bad_debt_ind,
        case
            when lower('extern_ar_flag_yn') = 'y'
            then 1
            else 0
        end as external_ar_ind,
        case
            when lower('claim_accepted_flg') = 'y'
            then 1
            else 0
        end as claim_accepted_ind,
        curr_auto_wo_tx_id as current_auto_write_off_tx_id,
        rvse_auto_wo_tx_id as reversal_auto_write_off_tx_id,
        act_na_woff_amt as actual_not_allowed_write_off_amount,
        exp_na_woff_amt as expected_not_allowed_write_off_amount,
        zc_bkt_type_ha.name as liability_bucket_type,
        zc_bkt_sts_ha.name as liability_bucket_status,
        zc_interim_type_ha.name as interim_liability_bucket_type,
        zc_close_reason_ha.name as close_reason,
        zc_woff_adj_sts_ha.name as write_off_adjustment_status,
        zc_claim_type_ha.name as claim_type,
        zc_mdcre_b_clm_flg.name as medicare_claim,
        {{
            dbt_utils.surrogate_key(check_columns or [] )
        }} as hash_value,
        'CLARITY' || '~' || bucket_id as integration_id,
        current_timestamp as create_date,
        'CLARITY' as create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        {{source('clarity_ods','hsp_bucket')}} as hsp_bucket
        left join {{source('clarity_ods','zc_bkt_type_ha')}} as zc_bkt_type_ha
            on zc_bkt_type_ha.bkt_type_ha_c = hsp_bucket.bkt_type_ha_c
        left join {{source('clarity_ods','zc_bkt_sts_ha')}} as zc_bkt_sts_ha
            on zc_bkt_sts_ha.bkt_sts_ha_c = hsp_bucket.bkt_sts_ha_c
        left join {{source('clarity_ods','zc_interim_type_ha')}} as zc_interim_type_ha
            on zc_interim_type_ha.interim_type_c = hsp_bucket.interim_type_c
        left join {{source('clarity_ods','zc_close_reason_ha')}} as zc_close_reason_ha 
            on zc_close_reason_ha.close_reason_ha_c = hsp_bucket.close_reason_ha_c
        left join {{source('clarity_ods','zc_woff_adj_sts_ha')}} as zc_woff_adj_sts_ha
            on zc_woff_adj_sts_ha.wo_adj_status_c = hsp_bucket.wo_adj_status_c
        left join {{source('clarity_ods','zc_claim_type_ha')}} as zc_claim_type_ha
            on zc_claim_type_ha.claim_type_ha_c = hsp_bucket.claim_type_ha_c
        left join {{source('clarity_ods','zc_mdcre_b_clm_flg')}} as zc_mdcre_b_clm_flg
            on zc_mdcre_b_clm_flg.mdcre_b_clm_flg_c = hsp_bucket.mdcre_b_clm_flg_c
    union all
 
    select
        -2 as liability_bucket_key,
        -2 as liability_bucket_id,
        null as liability_bucket_name,
        null as liabilty_bucket_number,
        -2 as hsp_account_id,
        -2 as hospital_account_key,
        -2 as payor_key,
        -2 as coverage_key,
        -2 as benefit_plan_key,
        -2 as service_area_key,
        null as current_balance,
        null as previous_credits,
        null as charge_total,
        null as charge_reversal_total,
        null as payment_total,
        null as adjustment_total,
        null as next_responsible_amount,
        null as first_claim_date,
        null as last_claim_date,
        null as interim_start_date,
        null as interim_end_date,
        null as bucket_create_date,
        0 as open_remark_bdc_ind,
        0 as open_denial_bdc_ind,
        0 as open_correspondence_bdc_ind,
        0 as bad_debt_ind,
        0 as external_ar_ind,
        0 as claim_accepted_ind,
        null as current_auto_write_off_tx_id,
        null as reversal_auto_write_off_tx_id,
        null as actual_not_allowed_write_off_amount,
        null as expected_not_allowed_write_off_amount,
        null as liability_bucket_type,
        null as liability_bucket_status,
        null as interim_liability_bucket_type,
        null as close_reason,
        null as write_off_adjustment_status,
        null as claim_type,
        null as medicare_claim,
        -2 as hash_value,
        'NOT APPLICABLE' as integration_id,
        current_timestamp as create_date,
        'NOT APPLICABLE' as create_source,
        current_timestamp as update_date,
        'NOT APPLICABLE' as update_source
 
    union all
 
    select
        -1 as liability_bucket_key,
        -1 as liability_bucket_id,
        null as liability_bucket_name,
        null as liabilty_bucket_number,
        -2 as hsp_account_id,
        -2 as hospital_account_key,
        -1 as payor_key,
        -1 as coverage_key,
        -1 as benefit_plan_key,
        -1 as service_area_key,
        null as current_balance,
        null as previous_credits,
        null as charge_total,
        null as charge_reversal_total,
        null as payment_total,
        null as adjustment_total,
        null as next_responsible_amount,
        null as first_claim_date,
        null as last_claim_date,
        null as interim_start_date,
        null as interim_end_date,
        null as bucket_create_date,
        0 as open_remark_bdc_ind,
        0 as open_denial_bdc_ind,
        0 as open_correspondence_bdc_ind,
        0 as bad_debt_ind,
        0 as external_ar_ind,
        0 as claim_accepted_ind,
        null as current_auto_write_off_tx_id,
        null as reversal_auto_write_off_tx_id,
        null as actual_not_allowed_write_off_amount,
        null as expected_not_allowed_write_off_amount,
        null as liability_bucket_type,
        null as liability_bucket_status,
        null as interim_liability_bucket_type,
        null as close_reason,
        null as write_off_adjustment_status,
        null as claim_type,
        null as medicare_claim,
        -1 as hash_value,
        'UNSPECIFIED' as integration_id,
        current_timestamp as create_date,
        'UNSPECIFIED' as create_source,
        current_timestamp as update_date,
        'UNSPECIFIED' as update_source
 
    -- add union all for each additional source if any
)
 
select
    liability_bucket_key,
    liability_bucket_id,
    liability_bucket_name,
    liabilty_bucket_number,
    hsp_account_id,
    hospital_account_key,
    payor_key,
    coverage_key,
    benefit_plan_key,
    service_area_key,
    current_balance,
    previous_credits,
    charge_total,
    charge_reversal_total,
    payment_total,
    adjustment_total,
    next_responsible_amount,
    first_claim_date,
    last_claim_date,
    interim_start_date,
    interim_end_date,
    bucket_create_date,
    open_remark_bdc_ind,
    open_denial_bdc_ind,
    open_correspondence_bdc_ind,
    bad_debt_ind,
    external_ar_ind,
    claim_accepted_ind,
    current_auto_write_off_tx_id,
    reversal_auto_write_off_tx_id,
    actual_not_allowed_write_off_amount,
    expected_not_allowed_write_off_amount,
    liability_bucket_type,
    liability_bucket_status,
    interim_liability_bucket_type,
    close_reason,
    write_off_adjustment_status,
    claim_type,
    medicare_claim,
    hash_value,
    integration_id,
    create_date,
    create_source,
    update_date,
    update_source
from
    unionset
where
    1 = 1
    {%- if is_incremental() %}
        and hash_value not in
        (
            select
                hash_value
            from
                {{ this }} -- TDL dim table
            where integration_id = unionset.integration_id
        )
    {%- endif %}