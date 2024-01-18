-- purpose: union legacy theradoc data with epic bugsy data of visits associated with an infection
-- granularity: one row per infection event per visit per micro result

with theradoc as (
    select
        infection_surveillance.inf_surv_key,
        visit.visit_key,
        visit.enc_id,
        row_number() over(partition by td_custom_infect_vw.c54_td_ica_surv_id order by visit.visit_key) as seq_num,
        td_custom_infect_vw.c26_account_number,
        td_custom_infect_vw.c54_td_ica_surv_id as inf_surv_id,
        td_custom_infect_vw.c61_rank_encounter as visit_rank,
        current_date as create_dt,
        'THERADOC' as create_by,
        current_date as upd_dt,
        'THERADOC' as upd_by

    from
        {{source('clarity_ods', 'td_custom_infect_vw')}} as td_custom_infect_vw
        left join {{ref('infection_surveillance')}} as infection_surveillance
            on td_custom_infect_vw.c54_td_ica_surv_id = infection_surveillance.inf_surv_id
        left join {{ref('visit')}} as visit
            on td_custom_infect_vw.c26_account_number = visit.enc_id
),

bugsy as (
    select
        infection_surveillance.inf_surv_key,
        visit.visit_key,
        visit.enc_id,
        row_number() over(partition by bugsy_custom_infect_vw.c54_td_ica_surv_id order by visit.visit_key) as seq_num,
        bugsy_custom_infect_vw.c26_account_number,
        bugsy_custom_infect_vw.c54_td_ica_surv_id as inf_surv_id,
        bugsy_custom_infect_vw.c61_rank_encounter as visit_rank,
        current_date as create_dt,
        'BUGSY' as create_by,
        current_date as upd_dt,
        'BUGSY' as upd_by

    from
        {{ref('bugsy_custom_infect_vw')}} as bugsy_custom_infect_vw
        left join {{ref('infection_surveillance')}} as infection_surveillance
            on bugsy_custom_infect_vw.c54_td_ica_surv_id = infection_surveillance.inf_surv_id
        left join {{ref('visit')}} as visit
            on bugsy_custom_infect_vw.c26_account_number = visit.enc_id
)

select
    inf_surv_key,
    inf_surv_id,
    seq_num,
    visit_key,
    enc_id::bigint as enc_id,
    visit_rank,
    create_dt::timestamp as create_dt,
    create_by::varchar(20) as create_by,
    upd_dt::timestamp as upd_dt,
    upd_by::varchar(20) as upd_by

from
    theradoc

union all

select
    inf_surv_key,
    inf_surv_id,
    seq_num,
    visit_key,
    enc_id::bigint as enc_id,
    visit_rank,
    create_dt::timestamp as create_dt,
    create_by::varchar(20) as create_by,
    upd_dt::timestamp as upd_dt,
    upd_by::varchar(20) as upd_by

from
    bugsy
