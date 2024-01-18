{{
    config(
        materialized = 'view',
        meta = {
            'critical': true
        }
    )
}}

select
    prov_block_avail_id,
    slot_begin_time,
    prov_id,
    department_id,
    pat_enc_csn_id,
    slot_day_unavail_ind as slot_day_unavail_yn,
    slot_tm_unavail_ind as slot_tm_unavail_yn,
    slot_lgth_min,
    appt_num_reg,
    dict_slot_unavail_rsn_c,
    dict_cat_slot_unavail_c,
    dict_slot_held_rsn_c,
    dict_cat_slot_held_c,
    filled_ind,
    manual_close_ind,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
from
    {{ ref('provider_availability_snapshot')}}
