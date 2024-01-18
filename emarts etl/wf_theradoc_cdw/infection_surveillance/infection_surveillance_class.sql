-- purpose: union legacy theradoc data with epic bugsy data of infection classes
-- granularity: one row per infection event per infection class

-- get max number of lines needed to pivot a single theradoc class into rows
with theradoc_long as (
    select distinct master_date.day_of_mm as lines

    from
        {{source('cdw', 'master_date')}} as master_date

    where
        master_date.day_of_mm <= 17 -- manual input for now
),

theradoc as (
    select
        infection_surveillance.inf_surv_key,
        td_custom_infection_classes.c54_td_ica_surv_id as inf_surv_id,
        row_number() over(partition by td_custom_infection_classes.c54_td_ica_surv_id order by inf_surv_cls_grp, inf_surv_cls_nm) as seq_num,
        td_custom_infection_classes.column_value as inf_surv_cls_grp,
        -- split full class string (inf_surv_cls_grp) into rows (inf_surv_cls_nm) on each pipe-delimiter
        case
            when theradoc_long.lines = 1
            then substr(td_custom_infection_classes.column_value, 1, instr(td_custom_infection_classes.column_value, '|', 1, theradoc_long.lines) - 1)
            else substr(td_custom_infection_classes.column_value, instr(td_custom_infection_classes.column_value, '|', 1, theradoc_long.lines - 1) + 1,
                case
                    when instr(td_custom_infection_classes.column_value, '|', 1, theradoc_long.lines) - instr(td_custom_infection_classes.column_value, '|', 1, theradoc_long.lines - 1) - 1 > 0
                    then instr(td_custom_infection_classes.column_value, '|', 1, theradoc_long.lines) - instr(td_custom_infection_classes.column_value, '|', 1, theradoc_long.lines - 1) - 1
                    else length(td_custom_infection_classes.column_value)
                end
            )
        end as inf_surv_cls_nm,
        -- split each inf_surv_cls_nm into substrings of before (inf_surv_cls_nm_mod) and after (inf_surv_cls_ansr) the '='
        case
            when inf_surv_cls_nm is null or length(inf_surv_cls_nm) = 0
            then substr(inf_surv_cls_grp, 1, instr(inf_surv_cls_grp, '=', -1) - 1)
            else substr(inf_surv_cls_nm, 1, instr(inf_surv_cls_nm, '=', -1) - 1)
        end as inf_surv_cls_nm_mod,
        case
            when inf_surv_cls_nm is null or length(inf_surv_cls_nm) = 0
            then substr(inf_surv_cls_grp, instr(inf_surv_cls_grp, '=', -1) + 1, length(inf_surv_cls_grp))
            else substr(inf_surv_cls_nm, instr(inf_surv_cls_nm, '=', -1) + 1, length(inf_surv_cls_nm))
        end as inf_surv_cls_ansr,
        current_date as create_dt,
        'THERADOC' as create_by,
        current_date as upd_dt,
        'THERADOC' as upd_by


    from
        {{source('clarity_ods', 'td_custom_infection_classes')}} as td_custom_infection_classes
        cross join {{ref('infection_surveillance')}} as infection_surveillance
        cross join theradoc_long

    where
        length(td_custom_infection_classes.column_value) - length(replace(td_custom_infection_classes.column_value, '|', '')) + 1 >= theradoc_long.lines
        and td_custom_infection_classes.c54_td_ica_surv_id = infection_surveillance.inf_surv_id
        and infection_surveillance.create_by = 'THERADOC'
),

bugsy as (
    select
        infection_surveillance.inf_surv_key,
        bugsy_custom_infection_classes.c54_td_ica_surv_id as inf_surv_id,
        row_number() over(partition by bugsy_custom_infection_classes.c54_td_ica_surv_id order by inf_surv_cls_grp, inf_surv_cls_nm) as seq_num,
        bugsy_custom_infection_classes.infection_class || '=Y' as inf_surv_cls_grp,
        bugsy_custom_infection_classes.infection_class as inf_surv_cls_nm,
        'Y' as inf_surv_cls_ansr,
        current_date as create_dt,
        'BUGSY' as create_by,
        current_date as upd_dt,
        'BUGSY' as upd_by

    from
        {{ref('bugsy_custom_infection_classes')}} as bugsy_custom_infection_classes
        left join {{ref('infection_surveillance')}} as infection_surveillance
            on bugsy_custom_infection_classes.c54_td_ica_surv_id = infection_surveillance.inf_surv_id

    where
        infection_surveillance.create_by = 'BUGSY'
)

select
    inf_surv_key,
    inf_surv_id,
    seq_num,
    inf_surv_cls_grp::varchar(4000) as inf_surv_cls_grp,
    inf_surv_cls_nm_mod::varchar(4000) as inf_surv_cls_nm,
    inf_surv_cls_ansr::varchar(100) as inf_surv_cls_ansr,
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
    inf_surv_cls_grp::varchar(4000) as inf_surv_cls_grp,
    inf_surv_cls_nm::varchar(4000) as inf_surv_cls_nm,
    inf_surv_cls_ansr::varchar(100) as inf_surv_cls_ansr,
    create_dt::timestamp as create_dt,
    create_by::varchar(20) as create_by,
    upd_dt::timestamp as upd_dt,
    upd_by::varchar(20) as upd_by

from
    bugsy
