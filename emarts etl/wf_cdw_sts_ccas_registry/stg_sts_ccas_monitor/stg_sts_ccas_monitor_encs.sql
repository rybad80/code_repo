with visit_ed_event_cte as (
    select
        evt.visit_key,
        evttype.event_id,
        evt.event_dt
    from
        {{source('cdw','visit_ed_event')}} as evt
    inner join {{source('cdw','master_event_type')}} as evttype
        on evt.event_type_key = evttype.event_type_key
    where
        evttype.event_id in
        ('1120000051', '1120000008', '1120000049', '1120000002', '100248', '1120000046', '1120000015')
),
visit_ed_event_max_event_dt_cte as (
    select
        visit_ed_event_cte.visit_key,
        visit_ed_event_cte.event_id,
        max(visit_ed_event_cte.event_dt) as event_dt
    from
        visit_ed_event_cte
    group by
        1, 2
)
select
    aneslink.or_log_key,
    or_log.log_key,
    anes_key,
    anes_event_visit_key,
    anes_visit_key,
    or_case.or_case_key,
    or_log_visit_key,
    proc_visit_key,
    aneslink.visit_key,
    vsi_anes.vsi_key as anes_vsi_key,
    vai_hsp.vsi_key as hsp_vai_key,
    pat.pat_mrn_id,
    or_log.log_id,
    to_char(aneslink.anes_start_tm, 'MM/DD/YYYY HH24:MI') as anes_start_dttm,
    aneslink.anes_start_tm as anes_start_tm,
    aneslink.anes_end_tm as anes_end_tm,
    case when tee.visit_key is null then 2 else 1 end as tee, -- coalesce(tee, 2) as tee,
    max(case
        when visit_ed_event_max_event_dt_cte.event_id = '1120000008' then visit_ed_event_max_event_dt_cte.event_dt
    end) as induct_tm, -- induct.induct_tm  
    isnull(
        to_char(induct_tm, 'MM/DD/YYYY HH24:MI'),
        to_char(aneslink.anes_start_tm, 'MM/DD/YYYY HH24:MI')
    ) as induction_dttm,
    max(case
        when visit_ed_event_max_event_dt_cte.event_id = '1120000049' then visit_ed_event_max_event_dt_cte.event_dt
    end) as anesready_tm, -- anesready.anesready_tm,
    max(case
        when visit_ed_event_max_event_dt_cte.event_id = '1120000002' then visit_ed_event_max_event_dt_cte.event_dt
    end) as anesstop_tm, -- anesstop.anesstop_tm,
    max(case
        when visit_ed_event_max_event_dt_cte.event_id = '100248' then visit_ed_event_max_event_dt_cte.event_dt
    end) as procstop_tm, -- procstop.procstop_tm,
    to_char(anesready_tm, 'MM/DD/YYYY HH24:MI') as anes_ready_dttm,
    coalesce(max(case
        when visit_ed_event_max_event_dt_cte.event_id = '1120000046' then visit_ed_event_max_event_dt_cte.event_dt
    end), anesstop_tm) as handoff_tm, -- COALESCE(handoff.handoff_tm, anesstop_tm) as handoff_tm,
    to_char(
        max(case
        when visit_ed_event_max_event_dt_cte.event_id = '1120000046' then visit_ed_event_max_event_dt_cte.event_dt end),
        'MM/DD/YYYY HH24:MI') as handoff_dttm, -- to_char(handoff.handoff_tm, 'MM/DD/YYYY HH24:MI') as handoff_dttm,
    max(case
        when visit_ed_event_max_event_dt_cte.event_id = '1120000015' then visit_ed_event_max_event_dt_cte.event_dt
    end) as anes_stop_doc_tm -- anes_stop_doc.anes_stop_doc_tm
    from
        {{source('cdw','anesthesia_encounter_link')}} as aneslink
        inner join {{source('cdw','or_case')}} as or_case on aneslink.or_case_key = or_case.or_case_key
        inner join {{source('cdw','or_log')}} as or_log on or_case.log_key = or_log.log_key
        left join {{source('cdw','visit_addl_info')}} as vai_hsp on vai_hsp.visit_key = aneslink.visit_key
        left join {{source('cdw','visit_stay_info')}} as vsi_anes on vsi_anes.visit_key = aneslink.anes_visit_key
        left join {{source('cdw','patient')}} as pat on pat.pat_key = or_case.pat_key
        inner join {{source('cdw','or_log_anes_staff')}} as anesstaff on anesstaff.log_key = or_log.log_key
        inner join {{source('cdw','cdw_dictionary')}} as service on service.dict_key = or_case.dict_or_svc_key
        left join visit_ed_event_max_event_dt_cte on visit_ed_event_max_event_dt_cte.visit_key = aneslink.anes_visit_key
        left join visit_ed_event_cte as tee on tee.visit_key = aneslink.anes_visit_key and tee.event_id = '1120000051'
    where
        anesstaff.dict_or_anes_type_key = 241354
        and vsi_anes.vsi_key > 0
    group by --noqa: L054
        aneslink.or_log_key,
        or_log.log_key,
        anes_key,
        anes_event_visit_key,
        anes_visit_key,
        or_case.or_case_key,
        or_log_visit_key,
        proc_visit_key,
        aneslink.visit_key,
        vsi_anes.vsi_key,
        vai_hsp.vsi_key,
        pat.pat_mrn_id,
        or_log.log_id,
        anes_start_dttm,
        aneslink.anes_start_tm,
        aneslink.anes_end_tm,
        tee
