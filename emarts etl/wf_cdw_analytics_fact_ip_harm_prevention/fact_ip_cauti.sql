-------------------------------------------
-- Code to generate FACT_IP_CAUTI
-------------------------------------------
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
    pat_mrn_id,
    pat_last_nm,
    pat_first_nm,
    pat_full_nm,
    pat_dob,
    pat_sex,
    pat_age_at_event,
    svc_nm,
    room_nm,
    room_num,
    bed_nm,
    num_days_admit_to_event,
    pathogen_code_1,
    pathogen_desc_1,
    pathogen_code_2,
    pathogen_desc_2,
    pathogen_code_3,
    pathogen_desc_3,
    urinary_catheter_status,
    event_dt,
    conf_dt,
    admit_dt,
    insertion_dt,
    international_ind,
    reportable_ind,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from (
    with lda_info as (
        select * from (
            select
foley.visit_key,
            foley.pat_lda_key,
            foley.place_dt,
            foley.remove_dt,
            row_number() over (partition by foley.visit_key order by foley.pat_lda_key) as lda_rank
            from {{source('cdw_analytics', 'fact_ip_lda_foley')}} as foley
        ) as lda_1 where lda_rank = 1 --noqa: L025
    )
    select
        coalesce(h.eventid, -1) as hai_event_id,
        coalesce(v.visit_key, -1) as visit_key,
        coalesce(h.pat_key, -1) as pat_key,
        coalesce(m.historical_dept_key, d.dept_key, -1) as dept_key,
        coalesce(v.room_key, -1) as room_key,
        coalesce(v.bed_key, -1) as bed_key,
        coalesce(s.inf_surv_key, -1) as inf_surv_key,
        coalesce(v.adt_svc_key, -2) as dict_svc_key,
        coalesce(foley.pat_lda_key, -1) as pat_lda_key,
        cast(coalesce(m.historical_dept_id, d.dept_id, -1) as bigint) as dept_id,
        cast(v.enc_id as numeric(14, 3)) as enc_id,
        cast(p.pat_mrn_id as varchar(25)) as pat_mrn_id,
        cast(p.last_nm as varchar(100)) as pat_last_nm,
        cast(p.first_nm as varchar(100)) as pat_first_nm,
        cast(p.full_nm as varchar(200)) as pat_full_nm,
        p.dob as pat_dob,
        p.sex as pat_sex,
        h.ageatevent as pat_age_at_event,
        cast(v.adt_svc_nm as varchar(50)) as svc_nm,
        v.room_nm,
        v.room_num,
        v.bed_nm,
        cast(h.admtoevntdays as integer) as num_days_admit_to_event,
        h.pathogen1 as pathogen_code_1,
        h.pathogendesc1 as pathogen_desc_1,
        h.pathogen2 as pathogen_code_2,
        h.pathogendesc2 as pathogen_desc_2,
        h.pathogen3 as pathogen_code_3,
        h.pathogendesc3 as pathogen_desc_3,
        h.urinarycath as urinary_catheter_status,
        h.eventdate as event_dt,
        coalesce(s.conf_dt, h.eventdate) as conf_dt,
        h.admitdate as admit_dt,
        coalesce(foley.place_dt, h.admitdate) as insertion_dt,
        --, TO_DATE(sc.INF_SURV_CLS_ANSR, 'MM/DD/YYYY') as INSERTION_DT
        cast(coalesce(v.international_ind, -2) as byteint) as international_ind,
        cast(1 as byteint) as reportable_ind,
        row_number() over (partition by h.eventid
                             order by case when v.dept_key = h.dept_key then 1 else 0 end desc,
                                      coalesce(v.bed_key, 0) desc,
                                      v.adt_svc_key desc
         ) as rownum
    from
        {{source('cdw_analytics', 'metrics_hai')}} as h
        left join {{source('cdw', 'patient')}} as p on p.pat_key = h.pat_key
        left join {{ref('stg_visit_event_service')}} as v on v.pat_key = h.pat_key and h.eventdate between v.enter_dt and v.exit_dt
        left join {{source('cdw', 'infection_surveillance')}} as s on h.eventid = s.inf_surv_id
        --left join CDW.INFECTION_SURVEILLANCE_CLASS sc            on s.INF_SURV_KEY = sc.INF_SURV_KEY and upper(sc.inf_surv_cls_nm) like '%INSERTION DATE%'
        left join {{source('cdw', 'department')}} as d on d.dept_key = h.dept_key
        left join {{source('cdw', 'lda_info')}} as foley on v.visit_key = foley.visit_key and (h.eventdate between foley.place_dt and foley.remove_dt or h.eventdate >= foley.place_dt and foley.remove_dt is null)
        --left join PATIENT_LDA pl                                on v.VISIT_KEY = pl.VISIT_KEY
        left join {{ref('master_harm_prevention_dept_mapping')}} as m
            on m.harm_type = 'CAUTI'
            and m.current_dept_key = d.dept_key
            and h.eventdate between m.start_dt and m.end_dt
            and m.denominator_only_ind = 0
    where
        h.hai_type = 'CAUTI'
) as cauti --noqa: L025
where
    rownum = 1
