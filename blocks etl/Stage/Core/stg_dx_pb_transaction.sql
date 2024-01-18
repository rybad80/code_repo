{{ config(
    materialized='table',
    dist='encounter_key',
    meta = {
        'critical': true
    }
) }}

with provider_billing as (
    select
        patient_id as pat_id,
        service_date,
        pat_enc_csn_id,
        tx_id,
        primary_dx_id,
        dx_two_id,
        dx_three_id,
        dx_four_id,
        dx_five_id,
        dx_six_id
    from
        {{source('clarity_ods','arpb_transactions')}}
    where
        tx_type_c = 1
        and void_date is null
        and primary_dx_id is not null
),

combined as (
    select
        pat_id,
        service_date,
        pat_enc_csn_id,
        primary_dx_id as dx_id,
        tx_id
    from
        provider_billing
    where
        primary_dx_id is not null
    union all
    select
        pat_id,
        service_date,
        pat_enc_csn_id,
        dx_two_id,
        tx_id
    from
        provider_billing
    where
        dx_two_id is not null
    union all
    select
        pat_id,
        service_date,
        pat_enc_csn_id,
        dx_three_id,
        tx_id
    from
        provider_billing
    where
        dx_three_id is not null
    union all
    select
        pat_id,
        service_date,
        pat_enc_csn_id,
        dx_four_id,
        tx_id
    from
        provider_billing
    where
        dx_four_id is not null
    union all
    select
        pat_id,
        service_date,
        pat_enc_csn_id,
        dx_five_id,
        tx_id
    from
        provider_billing
    where
        dx_five_id is not null
    union all
    select
        pat_id,
        service_date,
        pat_enc_csn_id,
        dx_six_id,
        tx_id
    from
        provider_billing
    where
        dx_six_id is not null
)

select
    stg_encounter.encounter_key,
    combined.tx_id,
    combined.pat_id,
    combined.service_date,
    combined.pat_enc_csn_id,
    combined.dx_id,
    row_number() over (partition by encounter_key, dx_id order by service_date) as enc_dx_row_num
from
    combined
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = combined.pat_enc_csn_id
group by
    stg_encounter.encounter_key,
    combined.tx_id,
    combined.pat_id,
    combined.service_date,
    combined.pat_enc_csn_id,
    combined.dx_id
