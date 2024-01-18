with lkp_vis_event1 as (
    select distinct -- noqa: L036
        a.visit_key
    from
        {{source('cdw', 'visit_ed_event')}} as a
        inner join {{source('cdw', 'master_event_type')}} as b on a.event_type_key = b.event_type_key
        inner join {{source('cdw', 'visit_addl_info')}} as c on a.visit_key = c.visit_key and c.dict_dspn_key > -2
    where
        b.event_nm = 'ED DEPART'
        and a.event_dt is not null
),
lkp_vis_event2 as (
    select distinct -- noqa: L036
        a.visit_key
    from
        {{source('cdw', 'visit_ed_event')}} as a
        inner join {{source('cdw', 'master_event_type')}} as b on a.event_type_key = b.event_type_key
        inner join {{source('cdw', 'visit_addl_info')}} as c on a.visit_key = c.visit_key and c.dict_dspn_key > -2
    where
        b.event_nm = 'ED ARRIVED'
        and a.event_dt is not null
)
select
    vis.visit_key,
    pat.pat_key,
    hav.hsp_acct_key,
    md.dx_key,
    dx.dx_ed_yn,
    dx.derived_dx_ed_yn,
    lkp_vis_event2.visit_key as event1_visit_key,
    lkp_vis_event1.visit_key as event2_visit_key,
    vee.visit_key as extended_visit_key,
    dx.primary_dx_yn,
    dx.line,
    dx.annotation,
    case when dx.contact_date < cast('2011-01-01 00:00:00' as timestamp) then 1 else 0 end as asap_ind
from
    {{ref('s_cl_pat_enc_dx')}} as dx
    left join {{source('cdw', 'visit')}} as vis on
        case
            when dx.pat_enc_csn_id is not null then dx.pat_enc_csn_id
            when 0 is not null then cast('0' as int8)
            else null::int8
        end = vis.enc_id
    left join {{source('cdw', 'patient')}} as pat on dx.pat_id = pat.pat_id
    left join {{source('cdw', 'master_diagnosis')}} as md on
        case
            when dx.dx_id is not null then dx.dx_id when 0 is not null then '0'::int8 else null::int8
        end = md.dx_id
    left join lkp_vis_event1 on vis.visit_key = lkp_vis_event1.visit_key
    left join {{source('cdw', 'hospital_account_visit')}} as hav on vis.visit_key = hav.visit_key
    left join lkp_vis_event2 on vis.visit_key = lkp_vis_event2.visit_key
    left join {{source('cdw', 'visit_ed_extended')}} as vee on vis.visit_key = vee.visit_key
where
    dx.contact_date >= to_date('07/01/2007', 'mm/dd/yyyy')
