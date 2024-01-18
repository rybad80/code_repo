{{ config(dist='pat_enc_csn_id') }}
with visit_cancel_date as (
    select
        pat_enc_csn_id,
        line,
        es_audit_time
    from (
        select
            pat_enc_csn_id,
            line,
            es_audit_time,
            max(line) over (partition by pat_enc_csn_id) as max_line
        from
            {{ source('clarity_ods', 'pat_enc_es_aud_act') }}
        where
            es_audit_action_c = 4
    ) as a
    where line = max_line
),
pat_enc_4_color_flag as (
    select
        pat_enc_csn_id,
        color_flag
    from
        {{ source('clarity_ods', 'pat_enc_4') }}
    where
        color_flag is not null
)

select
    p.pat_id,
    p.pat_enc_date_real::float as pat_enc_date_real,
    p.pat_enc_csn_id,
    p.contact_date,
    {{ clean_id('p', 'enc_type_c') }},
    p.enc_type_title,
    p.age,
    p.pcp_prov_id,
    fc.title as fin_class_c,
    p.visit_prov_id,
    p.visit_prov_title,
    p.department_id,
    p.bp_systolic,
    p.bp_diastolic,
    p.temperature,
    p.pulse,
    p.weight,
    p.height,
    p.respirations,
    p.lmp_date,
    lmp.title as lmp_other_c,
    p.head_circumference,
    p.enc_closed_yn,
    p.enc_closed_user_id,
    p.enc_close_date,
    p.los_prime_proc_id,
    eap.proc_code as los_proc_code,
    p.los_modifier1_id,
    p.los_modifier2_id,
    p.los_modifier3_id,
    p.los_modifier4_id,
    p.chkin_indicator_c,
    p.chkin_indicator_dt,
    {{ clean_id('p', 'appt_status_c') }},
    astat.title as appt_status_title,
    ab.title as appt_block_c,
    p.appt_time,
    p.appt_length,
    p.appt_made_date,
    p.appt_prc_id,
    p.checkin_time,
    p.checkout_time,
    p.arvl_lst_dl_time,
    p.arvl_lst_dl_usr_id,
    p.appt_entry_user_id,
    p.appt_canc_user_id,
    p.appt_cancel_date,
    p.checkin_user_id,
    p.cancel_reason_c,
    p.appt_serial_no,
    p.hosp_admsn_time,
    p.hosp_dischrg_time,
    at.title as hosp_admsn_type_c,
    p.noncvred_service_yn,
    p.referral_req_yn,
    p.referral_id,
    p.account_id,
    p.coverage_id,
    p.ar_episode_id,
    p.claim_id,
    p.primary_loc_id,
    p.charge_slip_number,
    cvg.payor_id as visit_epm_id,
    cvg.plan_id as visit_epp_id,
    cvg2.financial_class_c as visit_fc,
    p.copay_due,
    p.copay_collected,
    cs.name as copay_source_c,
    ct.title as copay_type_c,
    p.copay_ref_num,
    cpe.title as copay_pmt_expl_c,
    p.update_date,
    clarity_dep.serv_area_id, -- replacement column as part of the epic upgrade october 2021
    p.hsp_account_id,
    p.adm_for_surg_yn,
    p.surgical_svc_c,
    p.inpatient_data_id,
    p.ip_episode_id,
    p.appt_qnr_ans_id,
    p.attnd_prov_id,
    p.ordering_prov_text,
    os.title as es_order_status_c,
    p.external_visit_id,
    p.contact_comment,
    p.outgoing_call_yn,
    p.data_entry_person,
    p.is_walk_in_yn,
    p.cm_ct_owner_id,
    p.referral_source_id,
    p.sign_in_time,
    p.sign_in_user_id,
    p.appt_target_date,
    p.wc_tpl_visit_c,
    p.route_sum_prnt_yn,
    p.consent_type_c,
    p.phone_rem_stat_c,
    ac.abbr as appt_conf_stat_c,
    p.appt_conf_inst,
    p.hosp_license_c,
    p.accreditation_c,
    p.certification_c,
    p.entity_c,
    p.effective_date_dt,
    p.discharge_date_dt,
    p.effective_dept_id,
    p.cancel_reason_cmt,
    p.ordering_prov_id,
    p.bmi,
    p.bsa,
    p.avs_print_tm,
    p.avs_first_user_id,
    p.enc_med_frz_rsn_c,
    p.wc_tpl_visit_cmt,
    p.tobacco_use_vrfy_yn,
    p.phon_call_yn,
    p.phon_num_appt,
    p.enc_close_time,
    p.copay_pd_thru,
    p.interpreter_need_yn,
    p.vst_special_needs_c,
    p.intrp_assignment_c,
    p.asgnd_interp_type_c,
    p.interpreter_vend_c,
    p.interpreter_name,
    p.check_in_kiosk_id,
    p.benefit_package_id,
    p.benefit_comp_id,
    p.ben_adj_table_id,
    p.ben_adj_formula_id,
    p.ben_eng_sp_amt,
    p.ben_adj_copay_amt,
    p.ben_adj_method_c,
    p.entry_time,
    p.downtime_csn,
    p.enc_create_user_id,
    p.enc_instant,
    p.ed_arrival_kiosk_id,
    null as less72hr_pat_enc_csn_id,
    p2.research_study_id as research_studycode,
    p2.bill_num,
    vcd.es_audit_time,
    p2.adt_pat_class_c,
    p4.color_flag,
    p2.sup_prov_id as sup_prov_id,
    p2.sup_prov_c as sup_prov_c,
    p2.cosigner_user_id as cosigner_user_id,
    p2.cosign_rev_ins_dttm as cosign_rev_ins_dttm,
    p2.ip_doc_contact_csn as ip_doc_contact_csn,
    p2.msg_priority_c as msg_priority_c,
    phpf.bill_num as bill_num_from_pat_enc_hpf
from
    {{ source('clarity_ods', 'pat_enc') }} as p
    left join {{ source('clarity_ods', 'zc_financial_class') }} as fc on p.fin_class_c = fc.financial_class
    left join {{ source('clarity_ods', 'zc_lmp_other') }} as lmp on p.lmp_other_c = lmp.lmp_other_c
    left join {{ source('clarity_ods', 'zc_appt_status') }} as astat on p.appt_status_c = astat.appt_status_c
    left join {{ source('clarity_ods', 'zc_appt_block') }} as ab on p.appt_block_c = ab.appt_block_c
    left join {{ source('clarity_ods', 'zc_hosp_admsn_type') }} as at on p.hosp_admsn_type_c = at.hosp_admsn_type_c
    left join {{ source('clarity_ods', 'zc_payment_source') }} as cs on p.copay_source_c = cs.payment_source_c
    left join {{ source('clarity_ods', 'zc_copay_pmt_expl') }} as cpe on p.copay_pmt_expl_c = cpe.copay_pmt_expl_c
    left join {{ source('clarity_ods', 'zc_copay_type') }} as ct on p.copay_type_c = ct.copay_type_c
    left join {{ source('clarity_ods', 'zc_es_order_status') }} as os on p.es_order_status_c = os.es_order_status_c
    left join {{ source('clarity_ods', 'zc_appt_conf_stat') }} as ac on p.appt_conf_stat_c = ac.appt_conf_stat_c
    left join visit_cancel_date as vcd on p.pat_enc_csn_id = vcd.pat_enc_csn_id
    left join {{ source('clarity_ods', 'pat_enc_2') }} as p2 on p.pat_enc_csn_id = p2.pat_enc_csn_id
    left join {{ source('clarity_ods', 'pat_enc_hpf') }} as phpf on p.pat_enc_csn_id = phpf.pat_enc_csn_id
    left join pat_enc_4_color_flag as p4 on p.pat_enc_csn_id = p4.pat_enc_csn_id
    left join {{ source('clarity_ods', 'clarity_eap') }} as eap on eap.proc_id = p.los_prime_proc_id
    left join {{ source('clarity_ods', 'coverage') }} as cvg on cvg.coverage_id = p.coverage_id
    left join {{ source('clarity_ods', 'coverage_2') }} as cvg2 on cvg2.cvg_id = p.coverage_id
    left join {{ source('clarity_ods', 'clarity_dep') }} on clarity_dep.department_id = p.department_id
-- where 
    -- p.update_date > to_date('$$visit_next_ext_date', 'mm/dd/yyyy hh24:mi:ss')
