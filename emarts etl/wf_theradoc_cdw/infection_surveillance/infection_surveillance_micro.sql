-- purpose: union legacy theradoc data with epic bugsy data of organisms associated with an infection
-- granularity: one row per infection event per visit per micro result

with theradoc as (
    select
        infection_surveillance.inf_surv_key,
        row_number() over(partition by td_custom_infect_vw.c54_td_ica_surv_id order by c60_rank_micro, c47_order nulls last, c48_specimen_source, c50_test, c51_org_name, c49_specimen_source_category, c46_collect_date) as seq_num,
        td_custom_infect_vw.c46_collect_date as spec_coll_dt,
        td_custom_infect_vw.c47_order as inf_micro_nm,
        td_custom_infect_vw.c48_specimen_source as spec_src,
        td_custom_infect_vw.c49_specimen_source_category as spec_src_cat,
        td_custom_infect_vw.c50_test as test_nm,
        td_custom_infect_vw.c51_org_name as organism_nm,
        td_custom_infect_vw.c52_lab_result as lab_result,
        td_custom_infect_vw.c54_td_ica_surv_id as inf_surv_id,
        td_custom_infect_vw.c60_rank_micro as micro_rank,
        td_custom_infect_vw.c64_insert_location as insertion_location,
        current_date as create_dt,
        'THERADOC' as create_by,
        current_date as upd_dt,
        'THERADOC' as upd_by

    from
        {{source('clarity_ods', 'td_custom_infect_vw')}} as td_custom_infect_vw
        left join {{ref('infection_surveillance')}} as infection_surveillance
            on td_custom_infect_vw.c54_td_ica_surv_id = infection_surveillance.inf_surv_id
),

bugsy as (
    select
        infection_surveillance.inf_surv_key,
        row_number() over(partition by bugsy_custom_infect_vw.c54_td_ica_surv_id order by bugsy_custom_infect_vw.c60_rank_micro, c47_order, c48_specimen_source, c50_test, c51_org_name, c49_specimen_source_category, c46_collect_date) as seq_num,
        bugsy_custom_infect_vw.c46_collect_date as spec_coll_dt,
        bugsy_custom_infect_vw.c47_order as inf_micro_nm,
        bugsy_custom_infect_vw.c48_specimen_source as spec_src,
        bugsy_custom_infect_vw.c49_specimen_source_category as spec_src_cat,
        bugsy_custom_infect_vw.c50_test as test_nm,
        bugsy_custom_infect_vw.c51_org_name as organism_nm,
        bugsy_custom_infect_vw.c52_lab_result as lab_result,
        bugsy_custom_infect_vw.c54_td_ica_surv_id as inf_surv_id,
        bugsy_custom_infect_vw.c60_rank_micro as micro_rank,
        bugsy_custom_infect_vw.c64_insert_location as insertion_location,
        current_date as create_dt,
        'BUGSY' as create_by,
        current_date as upd_dt,
        'BUGSY' as upd_by

    from
        {{ref('bugsy_custom_infect_vw')}} as bugsy_custom_infect_vw
        left join {{ref('infection_surveillance')}} as infection_surveillance
            on bugsy_custom_infect_vw.c54_td_ica_surv_id = infection_surveillance.inf_surv_id
)

select
    inf_surv_key,
    inf_surv_id,
    seq_num,
    micro_rank,
    inf_micro_nm::varchar(500) as inf_micro_nm,
    spec_src::varchar(500) as spec_src,
    test_nm::varchar(500) as test_nm,
    organism_nm::varchar(500) as organism_nm,
    spec_src_cat::varchar(500) as spec_src_cat,
    lab_result::varchar(500) as lab_result,
    insertion_location::varchar(4000) as insertion_location,
    spec_coll_dt::timestamp as spec_coll_dt,
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
    micro_rank,
    inf_micro_nm::varchar(500) as inf_micro_nm,
    spec_src::varchar(500) as spec_src,
    test_nm::varchar(500) as test_nm,
    organism_nm::varchar(500) as organism_nm,
    spec_src_cat::varchar(500) as spec_src_cat,
    lab_result::varchar(500) as lab_result,
    insertion_location::varchar(4000) as insertion_location,
    spec_coll_dt::timestamp as spec_coll_dt,
    create_dt::timestamp as create_dt,
    create_by::varchar(20) as create_by,
    upd_dt::timestamp as upd_dt,
    upd_by::varchar(20) as upd_by

from
    bugsy
