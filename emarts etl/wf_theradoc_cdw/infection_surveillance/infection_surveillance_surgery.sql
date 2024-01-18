-- purpose: union legacy theradoc data with epic bugsy data of procedures associated with an infection
-- granularity: one row per infection event per surgical event

-- get surgical keys for theradoc and bugsy
with epic as (
    select
        or_log_all_procedures.log_key,
        or_log_all_procedures.seq_num,
        or_log_all_procedures.or_proc_key,
        or_log.log_id,
        ((or_log.log_id) || (or_log_all_procedures.seq_num)) as log_id_concat,
        or_log.pat_key

    from
        {{source('cdw', 'or_log_all_procedures')}} as or_log_all_procedures
        inner join {{source('cdw', 'or_log')}} as or_log
            on or_log_all_procedures.log_key = or_log.log_key

    where
        or_log_all_procedures.create_by = 'CLARITY'
),

-- get legacy surgical keys for theradoc
orm as (
    select
        min(or_log_all_procedures.log_key) as log_key,
        min(or_log_all_procedures.seq_num) as seq_num,
        min(or_log_all_procedures.or_proc_key) as or_proc_key,
        (replace(or_log.log_id, '.003', '')) as log_id_concat,
        or_log.pat_key,
        or_procedure.or_proc_abbr

    from
        {{source('cdw', 'or_log_all_procedures')}} as or_log_all_procedures
        inner join {{source('cdw', 'or_log')}} as or_log
            on or_log_all_procedures.log_key = or_log.log_key
        inner join {{source('cdw', 'or_procedure')}} as or_procedure
            on or_log_all_procedures.or_proc_key = or_procedure.or_proc_key

    where
        or_log_all_procedures.create_by = 'ORM'

-- align with legacy informatica granularity by adding group by statement
    group by
        log_id_concat,
        pat_key,
        or_proc_abbr
),

theradoc as (
    select
        infection_surveillance.inf_surv_key,
        row_number() over(partition by td_custom_infect_vw.c54_td_ica_surv_id order by td_custom_infect_vw.c59_rank_surgery, or_seq_num, log_key) as seq_num,
        decode(
            true,
            td_custom_infect_vw.c63_surgery_security_control is null, 0,
            epic.log_key is not null, epic.log_key,
            orm.log_key is not null, orm.log_key,
            -1
        ) as log_key,
        decode(
            true,
            td_custom_infect_vw.c63_surgery_security_control is null, 1,
            epic.seq_num is not null, epic.seq_num,
            orm.seq_num is not null, orm.seq_num,
            1
        ) as or_seq_num,
        decode(
            true,
            td_custom_infect_vw.c28_proc_code is null or td_custom_infect_vw.c63_surgery_security_control is null, 0,
            epic.or_proc_key is not null, epic.or_proc_key,
            orm.or_proc_key is not null, orm.or_proc_key,
            -1
        ) as or_proc_key,
        td_custom_infect_vw.c28_proc_code as nhsn_cat_nm,
        upper(td_custom_infect_vw.c29_proc_desc) as surg_desc,
        td_custom_infect_vw.c54_td_ica_surv_id as inf_surv_id,
        td_custom_infect_vw.c59_rank_surgery as surg_rank,
        td_custom_infect_vw.c63_surgery_security_control,
        current_date as create_dt,
        'THERADOC' as create_by,
        current_date as upd_dt,
        'THERADOC' as upd_by

    from
        {{source('clarity_ods', 'td_custom_infect_vw')}} as td_custom_infect_vw
        left join {{ref('infection_surveillance')}} as infection_surveillance
            on td_custom_infect_vw.c54_td_ica_surv_id = infection_surveillance.inf_surv_id
        left join epic
            on td_custom_infect_vw.c63_surgery_security_control = epic.log_id_concat
                and infection_surveillance.pat_key = epic.pat_key
        left join orm
            on td_custom_infect_vw.c63_surgery_security_control = orm.log_id_concat
                and td_custom_infect_vw.c28_proc_code = orm.or_proc_abbr
                and infection_surveillance.pat_key = orm.pat_key
),

bugsy as (
    select
        infection_surveillance.inf_surv_key,
        row_number() over(partition by bugsy_custom_infect_vw.c54_td_ica_surv_id order by bugsy_custom_infect_vw.c59_rank_surgery, or_seq_num) as seq_num,
        decode(
            true,
            bugsy_custom_infect_vw.c63_surgery_security_control is null, 0,
            epic.log_key is not null, epic.log_key,
            -1
        ) as log_key,
        decode(
            true,
            bugsy_custom_infect_vw.c63_surgery_security_control is null, 1,
            epic.seq_num is not null, epic.seq_num,
            1
        ) as or_seq_num,
        decode(
            true,
            bugsy_custom_infect_vw.c28_proc_code is null or bugsy_custom_infect_vw.c63_surgery_security_control is null, 0,
            epic.or_proc_key is not null, epic.or_proc_key,
            -1
        ) as or_proc_key,
        bugsy_custom_infect_vw.c28_proc_code as nhsn_cat_nm,
        upper(bugsy_custom_infect_vw.c29_proc_desc) as surg_desc,
        bugsy_custom_infect_vw.c54_td_ica_surv_id as inf_surv_id,
        bugsy_custom_infect_vw.c59_rank_surgery as surg_rank,
        bugsy_custom_infect_vw.c63_surgery_security_control,
        current_date as create_dt,
        'BUGSY' as create_by,
        current_date as upd_dt,
        'BUGSY' as upd_by

    from
        {{ref('bugsy_custom_infect_vw')}} as bugsy_custom_infect_vw
        left join {{ref('infection_surveillance')}} as infection_surveillance
            on bugsy_custom_infect_vw.c54_td_ica_surv_id = infection_surveillance.inf_surv_id
        left join epic
            on bugsy_custom_infect_vw.c63_surgery_security_control = epic.log_id
                and infection_surveillance.pat_key = epic.pat_key

    -- ensures each row aligns to the correct surgical keys and descriptions
    -- non-surgical cases are kept to avoid nulls downstream
    where
        or_seq_num = surg_rank
        or bugsy_custom_infect_vw.c63_surgery_security_control is null
)

select
    inf_surv_key,
    inf_surv_id,
    seq_num,
    log_key,
    or_seq_num,
    or_proc_key,
    surg_rank,
    nhsn_cat_nm::varchar(100) as nhsn_cat_nm,
    surg_desc::varchar(500) as surg_desc,
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
    log_key,
    or_seq_num,
    or_proc_key,
    surg_rank,
    nhsn_cat_nm::varchar(100) as nhsn_cat_nm,
    surg_desc::varchar(500) as surg_desc,
    create_dt::timestamp as create_dt,
    create_by::varchar(20) as create_by,
    upd_dt::timestamp as upd_dt,
    upd_by::varchar(20) as upd_by

from
    bugsy
