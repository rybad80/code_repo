select
    fv.visit_key as "Encounter Key",
    date(adm_ev.eff_event_dt) as "Inpatient Admission Date",
    adm_ev.eff_event_dt as "Inpatient Admission Time Hour",
    date(disch_ev.eff_event_dt) as "Inpatient Discharge Date",
    disch_ev.eff_event_dt as "Inpatient Discharge Time Hour",
    fv.ed_los_min as "ED Length of Stay Minutes",
    fv.disch_ord_room_clean_min as "Disch Order for Room Clean",
    fv.disch_11_elig_ind as "Disch Eligible by 11am Ind",
    fv.disch_11_ind as "Disch by 11am Ind",
    fv.md_rpt_ip_rn_paged_min as "MD Report to RN Paged Min",
    fv.ip_rn_paged_to_admit_min as "IP RN Call to Admit Date Min",
    fv.ip_rn_paged_ed_handoff_min as "IP RN Call to Handoff Rev Min",
    fv.ed_handoff_admit_min as "Ed Handoff to Admit Date Min",
    fv.disch_ord_room_vacancy_min as "Disch Order to Rm Vacancy Min",
    fv.room_vacancy_room_clean_min as "Rm Vacancy to Rm Clean Min",
    fv.room_vacancy_in_progress_min as "Rm Vacancy to InProgress Min",
    fv.in_progress_room_clean_min as "InProgress to Rm Clean Min",
    fv.bed_req_unit_assgn_min as "Bed Request to Unit Assign Min",
    fv.unit_assgn_bed_assgn_min as "Unit Assign to Bed Assign Min",
    fv.right_svc as "Right Service Indicator",
    fv.right_svc_elig as "Right Service Eligible Ind",
    dept_adm.dept_abbr as "Admitting Department",
    dept_disch.dept_abbr as "Discharge Department",
    dict1.dict_abbr as "Admitting Patient Class",
    dict2.dict_abbr as "Discharge Patient Class",
    dict3.dict_nm as "Admitting Patient Service",
    dict4.dict_nm as "Discharge Patient Service"
from
    {{source('cdw_analytics', 'fact_visit')}} as fv
    left join {{source('cdw', 'visit_event')}} as adm_ev on ((fv.adm_event_key = adm_ev.visit_event_key))
    left join {{source('cdw', 'visit_event')}} as disch_ev on ((fv.disch_event_key = disch_ev.visit_event_key))
    left join {{source('cdw', 'department')}} as dept_adm on ((adm_ev.dept_key = dept_adm.dept_key))
    left join {{source('cdw', 'department')}} as dept_disch on ((disch_ev.dept_key = dept_disch.dept_key))
    left join {{source('cdw', 'cdw_dictionary')}} as dict1 on ((adm_ev.dict_pat_class_key = dict1.dict_key))
    left join {{source('cdw', 'cdw_dictionary')}} as dict2 on ((disch_ev.dict_pat_class_key = dict2.dict_key))
    left join {{source('cdw', 'cdw_dictionary')}} as dict3 on ((adm_ev.dict_pat_svc_key = dict3.dict_key))
    left join {{source('cdw', 'cdw_dictionary')}} as dict4 on ((disch_ev.dict_pat_svc_key = dict4.dict_key))
