select
    vdoc.anatomy_abbr as "Anatomy Abbr",
    vdoc.anatomy_extnl_nm as "Anatomy External Name",
    vdoc.anatomy_id as "Anatomy Id",
    dict21.dict_nm as "Anatomy Type",
    dict27.dict_nm as "CDC Risk Donor",
    organ.orgn_cischemia_min as "Cold Ischemia Minute",
    info.dnr_max_wt as "Donor Maximum Weight",
    info.dnr_min_wt as "Donor Minimum Weight",
    dict18.dict_nm as "Donor Recipient Relation",
    dict24.dict_nm as "Donor Willing Indicator",
    info.summary_block_id as "Episode of Care Id",
    dict26.dict_nm as "Exhausted Vascular Access",
    dict25.dict_nm as "Exhuasted Peritoneal Access",
    dict32.dict_nm as "High Risk Donor Indicator",
    dict27.dict_nm as "Historic Transplant Indicator",
    dict6.dict_nm as "Historical Transplant Center",
    dict7.dict_nm as "Induction Used",
    organ.clamp_off_tm as "Instance Clamp Off Time",
    organ.clamp_on_tm as "Instance Clamp On Time",
    organ.intraop_transfusion as "Intr-operative Transfusion",
    dict30.dict_nm as "Kidney Biopsy Indicator",
    dict4.dict_nm as "Live Donor Possibility",
    organ.match_run as "Match Run Id",
    organ.natv_prim_other as "Native Organ Failure Prim Oth",
    dict8.dict_nm as "Native Organ Failure Prim",
    dict31.dict_nm as "Native Organ Indicator",
    dict10.dict_nm as "Organ Donor Criteria",
    organ.orgn_fail_dt as "Organ Fail Date",
    organ.natv_contrib_other as "Organ Failure Contributory",
    dict11.dict_nm as "Organ Failure Method",
    dict12.dict_nm as "Organ Match Type",
    dict15.dict_nm as "Organ Procedure Status",
    organ.orgn_procurement_dt as "Organ Procurement Date",
    dict9.dict_nm as "Organ Procurement Organization",
    dict29.dict_nm as "Organ Procuremnet External",
    dict14.dict_nm as "Organ Received On",
    organ.orgn_rec_id as "Organ Record Id",
    organ.orgn_rec_key as "Organ Record Key",
    dict19.dict_nm as "Organ Source",
    dict16.dict_nm as "Organ Stayed On",
    dict13.dict_nm as "Organ Transplant Technique",
    organ.preop_blood_transfusion as "Periop Transfusion Quantity",
    organ.periop_transfusion as "Periop Transfusion",
    organ.port_clamp_off_dt as "Portal Clamp Off Date",
    info.tm_to_cntr as "Time to Center",
    organ.orgn_tischemia_min as "Total Ischemia Min",
    info.adm_dt as "Transplant Admission Date",
    organ.anastomosis_start_dt as "Transplant Anastomosis Date",
    organ.antigen_match as "Transplant Antigen Matches",
    info.transplnt_cntr_wtlst_dt as "Transplant Cntr Waitlist Date",
    dict1.dict_nm as "Transplant Current Reason",
    dict2.dict_nm as "Transplant Current Stage",
    dict3.dict_nm as "Transplant Current Status",
    info.transplnt_disch_dt as "Transplant Discharge Date",
    dict5.dict_nm as "Transplant Episode Type",
    info.transplnt_eval_dt as "Transplant Evaluation Date",
    info.transplnt_next_rvw_dt as "Transplant Next Review Date",
    info.transplnt_num as "Transplant Number",
    info.transplnt_cur_stage_dt as "Transplant Phase Update Date",
    dict17.dict_nm as "Transplant Record Status",
    info.transplnt_rfl_dt as "Transplant Referral Date",
    info.transplnt_rvw_dt as "Transplant Review Date",
    info.transplnt_surg_dt as "Transplant Surgery Date",
    info.transplnt_wtlst_dt as "Transplant Waitlist Date",
    dict20.dict_nm as "UNOS Organ Fail Reason",
    dict23.dict_nm as "Vessel Location",
    vdoc.ves_rec_nm as "Vessel Received Name",
    vxref.seq_num as "Vessel Sequence Line",
    info.visit_key as "Visit Key",
    organ.orgn_wischemia_min as "Warm Ischemia Min"
from
    {{ source('cdw', 'transplant_info') }} info
    left join {{ source('cdw', 'transplant_organs') }} torg on ((torg.epsd_key = info.epsd_key))
    left join {{ source('cdw', 'organ') }} organ on ((organ.orgn_rec_key = torg.orgn_rec_key))
    left join {{ source('cdw', 'organ_vessel_xref') }} vxref on ((vxref.orgn_rec_key = organ.orgn_rec_key))
    left join {{ source('cdw', 'vessel_document') }} vdoc on ((vdoc.ves_rec_key = vxref.ves_rec_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict1 on (
        (
            dict1.dict_key = info.dict_transplnt_curr_rsn_key
        )
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict2 on (
        (
            dict2.dict_key = info.dict_transplnt_curr_stage_key
        )
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict3 on (
        (
            dict3.dict_key = info.dict_transplnt_curr_stat_key
        )
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict4 on (
        (dict4.dict_key = info.dict_transplnt_dnr_psn_key)
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict5 on (
        (
            dict5.dict_key = info.dict_transplnt_epsd_type_key
        )
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict6 on (
        (
            dict6.dict_key = info.dict_transplnt_hist_cntr_key
        )
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict7 on ((dict7.dict_key = organ.dict_induction_use_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict8 on ((dict8.dict_key = organ.dict_natv_prim_fail_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict9 on ((dict9.dict_key = organ.dict_opo_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict10 on ((dict10.dict_key = organ.dict_orgn_dnr_crit_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict11 on (
        (dict11.dict_key = organ.dict_orgn_fail_meth_key)
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict12 on (
        (dict12.dict_key = organ.dict_orgn_match_type_key)
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict13 on (
        (dict13.dict_key = organ.dict_orgn_proc_type_key)
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict14 on ((dict14.dict_key = organ.dict_orgn_rcvd_on_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict15 on ((dict15.dict_key = organ.dict_orgn_state_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict16 on (
        (dict16.dict_key = organ.dict_orgn_stayed_on_key)
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict17 on ((dict17.dict_key = organ.dict_rec_stat_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict18 on (
        (
            dict18.dict_key = organ.dict_transplnt_dnr_rel_key
        )
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict19 on ((dict19.dict_key = organ.dict_transplnt_src_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict20 on (
        (dict20.dict_key = organ.dict_unos_prim_fail_key)
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict21 on ((dict21.dict_key = vdoc.dict_anatomy_type_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict22 on ((dict22.dict_key = vdoc.dict_rec_stat_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict23 on ((dict23.dict_key = vdoc.dict_ves_loc_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict24 on (
        (dict24.dict_key = info.transplnt_dnr_willing_ind)
    )
    left join {{ source('cdw', 'cdw_dictionary') }} dict25 on ((dict25.dict_key = info.exhst_periton_ind))
    left join {{ source('cdw', 'cdw_dictionary') }} dict26 on ((dict26.dict_key = info.exhst_vasc_hem_ind))
    left join {{ source('cdw', 'cdw_dictionary') }} dict27 on ((dict27.dict_key = info.transplnt_hist_ind))
    left join {{ source('cdw', 'cdw_dictionary') }} dict28 on ((dict28.dict_key = organ.cdc_risk_ind))
    left join {{ source('cdw', 'cdw_dictionary') }} dict29 on ((dict29.dict_key = organ.extl_team_rcvr_ind))
    left join {{ source('cdw', 'cdw_dictionary') }} dict30 on ((dict30.dict_key = organ.kidney_biopsy_ind))
    left join {{ source('cdw', 'cdw_dictionary') }} dict31 on ((dict31.dict_key = organ.natv_orgn_ind))
    left join {{ source('cdw', 'cdw_dictionary') }} dict32 on ((dict32.dict_key = organ.opo_risk_ind))