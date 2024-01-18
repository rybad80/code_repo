select
hai_event_id,
visit_key,
pat_key,
dept_key,
room_key,
bed_key,
inf_surv_key,
dict_svc_key,
pat_lda_key,
dept_id,
enc_id,
room_id,
bed_id,
pat_mrn_id,
pat_last_nm,
pat_first_nm,
pat_full_nm,
pat_dob,
pat_sex,
svc_nm,
event_type,
pathogen_code_1,
pathogen_desc_1,
pathogen_code_2,
pathogen_desc_2,
pathogen_code_3,
pathogen_desc_3,
insertion_loc,
line_type,
birth_wt_in_grams,
birth_wt_code,
num_days_admit_to_event,
num_days_from_insertion,
room_nm,
room_num,
bed_nm,
event_dt,
conf_dt,
insertion_dt,
umbilical_catheter_ind,
ventilator_used_ind,
mbi_lcbi_ind,
international_ind,
reportable_ind,
current_timestamp as create_dt,
'DBT' as create_by,
current_timestamp as upd_dt
from (
    with surv_class_theradoc as (
        select
            class.inf_surv_key,
            micro.insertion_location as insertion_location,
            max(case when inf_surv_cls_nm like '%Insertion Date%' then inf_surv_cls_ansr end) as insertion_dt,
            max(case when upper(inf_surv_cls_nm) like '%MBI-LCBI%' then 1 else 0 end) as mbi_lcbi_ind,
            max(
                case
                    when inf_surv_cls_ansr like '%perm_broviac%' then 'Broviac   '
                    when inf_surv_cls_ansr like '%perm_hickman%' then 'Hickman   '
                    when inf_surv_cls_ansr like '%perm_port%' then 'Port-a-cath   '
                    when inf_surv_cls_ansr like '%perm_tunneled%' then 'Tunneled   '
                    when inf_surv_cls_ansr like '%perm_other%' then 'Permanent-Other   '
                    else '' end
                )
            || max(
                case
                    when inf_surv_cls_ansr like '%temp_femoral%' then 'Femoral   '
                    when inf_surv_cls_ansr like '%temp_peripheral%' then 'Peripherally Inserted(PICC)   '
                    when inf_surv_cls_ansr like '%temp_other%' then 'Temporary-Other   '
                    else '' end
                )
            || max(
                case
                    when inf_surv_cls_ansr like '%umb_arterial%' then 'Arterial   '
                    when inf_surv_cls_ansr like '%umb_venous%' then 'Venous   '
                    when inf_surv_cls_ansr like '%umb_other%' then 'Umbilical-Other   '
                else '' end
                )
            || max(
                case
                    when upper(inf_surv_cls_nm) like '%PERMANENT%IMPLANTED%' then 'Permanent   '
                    when upper(inf_surv_cls_nm) like '%TEMPORARY%INDWELLING%' then 'Temporary   '
                    when upper(inf_surv_cls_nm) like '%UMBILICAL%' then 'Umbilical   '
                    else 'Not Indicated' end
                )
            as line_type
        from {{source('cdw', 'infection_surveillance_class')}} as class
        left join {{source('cdw', 'infection_surveillance_micro')}} as micro on class.inf_surv_key = micro.inf_surv_key
        where class.create_by = 'THERADOC'
        group by class.inf_surv_key, micro.insertion_location
    ),
    surv_class_bugsy as (
        select
            class.inf_surv_key,
            micro.insertion_location as insertion_location,
            max(bugsy_custom_infection_classes.placement_insertion_dttm) as insertion_dt,
            max(case when bugsy_custom_infection_classes.mbi_lcbi_yn = 'Y' then 1 else 0 end) as mbi_lcbi_ind,
            null as line_type
        from {{source('cdw', 'infection_surveillance_class')}} as class
        left join {{source('cdw', 'infection_surveillance_micro')}} as micro on class.inf_surv_key = micro.inf_surv_key
        left join {{source('cdw', 'infection_surveillance')}} as infection_surveillance on infection_surveillance.inf_surv_key = micro.inf_surv_key
        left join {{source('cdw', 'bugsy_custom_infection_classes')}} as bugsy_custom_infection_classes on infection_surveillance.inf_surv_id = bugsy_custom_infection_classes.c54_td_ica_surv_id
        where class.create_by = 'BUGSY'
        group by class.inf_surv_key, micro.insertion_location
    ),
    surv_class as (
    select * from surv_class_bugsy
    union all
    select * from surv_class_theradoc
    ),
    risk as (
        select * from (
            select
                pl.visit_key,
                pl.pat_lda_key,
                min(
                    case when fslda.dict_lda_type_key = 20861 then 2
                    when fslda.dict_lda_type_key = 20862 and instr(upper(pl.lda_desc), 'INTERNAL JUGULAR') * instr(upper(pl.lda_desc), 'NON TUNNELED') != 0 then 1
                    when fslda.dict_lda_type_key = 20862 then 3
                    when fslda.dict_lda_type_key = 20870 then 4
                    else 5
               end) as risk_level,
                row_number() over (partition by visit_key order by risk_level, pat_lda_key) as rn
            from {{source('cdw', 'flowsheet_lda_group')}} as fslda
            left join {{source('cdw', 'patient_lda')}} as pl on fslda.fs_key = pl.fs_key
            group by pl.visit_key, pl.pat_lda_key
        ) as risk_2 where rn = 1 --noqa: L025
    )
    select
            coalesce(h.eventid, -1) as hai_event_id,
            coalesce(v.visit_key, h.visit_key, -1) as visit_key,
            coalesce(h.pat_key, -1) as pat_key,
            coalesce(m.historical_dept_key, d.dept_key, -1) as dept_key,
            coalesce(v.room_key, -1) as room_key,
            coalesce(v.bed_key, -1) as bed_key,
            coalesce(s.inf_surv_key, -1) as inf_surv_key,
            coalesce(v.adt_svc_key, -2) as dict_svc_key,
            coalesce(rs.pat_lda_key, -1) as pat_lda_key,
            coalesce(m.historical_dept_id, d.dept_id, -1) as dept_id,
            v.enc_id,
            v.room_id,
            v.bed_id,
            p.pat_mrn_id,
            p.last_nm as pat_last_nm,
            p.first_nm as pat_first_nm,
            p.full_nm as pat_full_nm,
            p.dob as pat_dob,
            p.sex as pat_sex,
            v.adt_svc_nm as svc_nm,
            h.eventtype as event_type,
            h.pathogen1 as pathogen_code_1,
            h.pathogendesc1 as pathogen_desc_1,
            h.pathogen2 as pathogen_code_2,
            h.pathogendesc2 as pathogen_desc_2,
            h.pathogen3 as pathogen_code_3,
            h.pathogendesc3 as pathogen_desc_3,
            sc.insertion_location as insertion_loc,
            sc.line_type,
            case when h.birthwt is not null then cast(h.birthwt as numeric(16, 5)) end as birth_wt_in_grams,
            trim(coalesce(h.birthwtcode, 'Missing')) as birth_wt_code,
            h.admtoevntdays as num_days_admit_to_event,
            case when sc.insertion_dt is not null then date(h.eventdate) - date(sc.insertion_dt) end as num_days_from_insertion,
            v.room_nm,
            v.room_num,
            v.bed_nm,
            h.eventdate as event_dt,
            date(s.conf_dt) as conf_dt,
            date(sc.insertion_dt) as insertion_dt,
            case when h.umbcatheter = 'Y' then 1 when h.umbcatheter = 'N' then 0 else -2 end as umbilical_catheter_ind,
            case when h.ventused = 'Y' then 1 when h.ventused = 'N' then 0 else -2 end as ventilator_used_ind,
            coalesce(sc.mbi_lcbi_ind, -2) as mbi_lcbi_ind,
            coalesce(v.international_ind, -2) as international_ind,
            --case when h.completedflag = 'Y' then 1 else 0 end as reportable_ind,
            case when h.completedflag = 'Y' and mbi_lcbi_ind = 0 then 1 else 0 end as reportable_ind,
            row_number() over (partition by h.eventid
                                 order by case when v.dept_key = h.dept_key then 1 else 0 end desc,
                                          coalesce(v.bed_key, 0) desc,
                                          v.adt_svc_key desc
             ) as rownum
    from
        {{source('cdw_analytics', 'metrics_hai')}} as h
        left join {{source('cdw', 'patient')}} as p on p.pat_key = h.pat_key
        left join {{ref('stg_visit_event_service')}} as v on v.pat_key = h.pat_key and date(h.eventdate) between date(v.enter_dt) and date(v.exit_dt)
        left join risk as rs on v.visit_key = rs.visit_key
        left join {{source('cdw', 'infection_surveillance')}} as s on h.eventid = s.inf_surv_id
        left join surv_class as sc on sc.inf_surv_key = s.inf_surv_key
        left join {{source('cdw', 'department')}} as d on d.dept_key = h.dept_key
        left join {{ref('master_harm_prevention_dept_mapping')}} as m
            on m.harm_type = 'CLABSI'
            and m.current_dept_key = d.dept_key
            and h.eventdate between m.start_dt and m.end_dt
            and m.denominator_only_ind = 0
    where
        h.hai_type = 'CLABSI'
        and date(h.eventdate) >= date('2010-07-01')
) as clabsi --noqa: L025
where rownum = 1
