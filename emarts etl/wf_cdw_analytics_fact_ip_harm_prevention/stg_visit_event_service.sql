with international_patients as (
    select distinct p.pat_key
    from {{source('cdw', 'patient')}} as p
    left join {{source('cdw', 'patient_primary_care_provider')}} as pcp on pcp.pat_key = p.pat_key
    left join {{source('cdw', 'provider')}} as prov on prov.prov_key = pcp.pcp_prov_key
    where pcp.pat_pcp_deleted_ind != 1
    and prov.prov_id in ('16298', '659102')
)
select
      vai.visit_key,
    vai.pat_key,
    v_enter.bed_key,
    v_enter.room_key,
    v_enter.dept_key as dept_key,
    COALESCE(dict_svc.dict_key, -2) as adt_svc_key,
    vai.enc_id,
    p.pat_mrn_id,
    mb.bed_id,
    mr.room_id,
    p.country as pat_country,
    mb.bed_nm,
    mr.room_num,
    mr.room_nm,
    dict_svc.dict_nm as adt_svc_nm,
    case when ip.pat_key is not null then 1 else 0 end as international_ind,
    case when dict_enter.src_id in (1, 3) then 1 else 0 end as admin_transfer_in_ind,
    vai.hosp_admit_dt,
    vai.hosp_disch_dt as hosp_dischrg_dt,
    v_enter.eff_event_dt as enter_dt,
    COALESCE(v_exit.eff_event_dt, NOW()) as exit_dt
from
    {{source('cdw', 'visit_addl_info')}} as vai
    inner join {{source('cdw', 'patient')}} as p on p.pat_key = vai.pat_key
    inner join {{source('cdw', 'visit_event')}} as v_enter on v_enter.visit_key = vai.visit_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_status on dict_status.dict_key = v_enter.dict_event_subtype_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_enter on dict_enter.dict_key = v_enter.dict_adt_event_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_svc on dict_svc.dict_key = v_enter.dict_pat_svc_key
    left join {{source('cdw', 'visit_event')}} as v_exit on v_exit.visit_event_key = v_enter.xfer_out_visit_event_key
    left join {{source('cdw', 'cdw_dictionary')}} as dict_exit on dict_exit.dict_key = v_exit.dict_adt_event_key
    left join {{source('cdw', 'master_bed')}} as mb on mb.bed_key = v_enter.bed_key
    left join {{source('cdw', 'master_room')}} as mr on mr.room_key = v_enter.room_key
    left join international_patients as ip on ip.pat_key = p.pat_key
    where
        dict_status.dict_nm != 'Canceled' -- Remove cancelled ADT events
        and (v_exit.eff_event_dt is not null or admin_transfer_in_ind = 1) -- If there is an exit_dt OR if there has been an admit, but no exit date yet
