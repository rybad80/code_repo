/*
{
  "subject_area": "clarity",
  "workflow_name": "wf_clarity_cdw_core_1",
  "worklet_name": "wklt_cdw_core_tier5",
  "session_name": "s_cdw_load_visit",
  "mapping_name": "m_cdw_load_visit",
  "mapping_id": 8172,
  "target_id": 9027,
  "target_name": "visit"
}
*/
 
with sq_s_pat_enc as (
    with visit_keys as (
        select
            visit_lookup.visit_key as visit_key,
            ip_doc_visit_lookup.visit_key as ip_doc_contact_csn_visit_key,
            stg_pat_enc.pat_enc_csn_id,
            stg_pat_enc.ip_doc_contact_csn
        from 
            {{ ref('stg_pat_enc') }} as stg_pat_enc
            left join {{ref('stg_visit_key_lookup')}} as visit_lookup
                on visit_lookup.encounter_id = stg_pat_enc.pat_enc_csn_id
                and visit_lookup.source_name = 'clarity'                             
            left join {{ref('stg_visit_key_lookup')}} as ip_doc_visit_lookup
                on ip_doc_visit_lookup.encounter_id = stg_pat_enc.ip_doc_contact_csn
                and ip_doc_visit_lookup.source_name = 'clarity'
    )
    select 
        coalesce(s.pat_id,'0') as pat_id,
        s.pat_enc_date_real,
        s.pat_enc_csn_id,
        coalesce(s.contact_date,to_date('12319999','mmddyyyy')) as contact_date,
        coalesce(s.enc_type_c,'0') as enc_type_c,
        s.age,
        coalesce(pcp_prov_id,'0') as pc_prov_id,
        case
            when lower(visit_prov_id) = 'error 448'
            then '-1'
            else coalesce(visit_prov_id,'0')
        end as visit_prov_id,
        coalesce(stg_pat_enc_hsp.discharge_prov_id, '0') as dischrg_prov_id,
        coalesce(s.department_id,0) as department_id,
        bp_systolic,
        bp_diastolic,
        temperature,
        s.pulse,
        weight,
        height,
        s.respirations,
        lmp_date,
        head_circumference,
        enc_closed_yn,
        coalesce(los_prime_proc_id,0) as los_prime_proc_id,
        los_proc_code,
        coalesce(los_modifier1_id, '0') as los_modifier1_id,
        coalesce(los_modifier2_id, '0') as los_modifier2_id,
        coalesce(los_modifier3_id, '0') as los_modifier3_id,
        coalesce(los_modifier4_id, '0') as los_modifier4_id,
        coalesce(chkin_indicator_c,'0') as chkin_indicator_c,
        coalesce(appt_status_c,0) as appt_status_c,
        appt_status_title,
        appt_block_c,
        appt_time,
        appt_length,
        appt_made_date,
        appt_prc_id,
        checkin_time,
        checkout_time,
        appt_entry_user_id,
        appt_cancel_date,
        checkin_user_id,
        cancel_reason_c,
        appt_serial_no,
        s.hosp_admsn_time,
        s.hosp_dischrg_time,
        s.hosp_admsn_type_c,
        s.referral_req_yn,
        coalesce(s.referral_id,0) as referral_id,
        coalesce(s.account_id,0) as account_id,
        s.coverage_id,
        s.claim_id,
        s.charge_slip_number,
        coalesce(s.visit_epm_id,0) as visit_epm_id,
        coalesce(s.visit_epp_id,0) as visit_epp_id,
        coalesce(s.visit_fc,'0') as visit_fc,
        s.copay_due,
        s.copay_collected,
        coalesce(s.copay_source_c, 'NOT APPLICABLE') as copay_source_c,
        s.copay_ref_num,
        coalesce(s.serv_area_id,0) as serv_area_id,
        s.hsp_account_id,
        s.adm_for_surg_yn,
        s.is_walk_in_yn,
        coalesce(s.referral_source_id,'0') as referral_source_id,
        s.phone_rem_stat_c,
        s.entity_c,
        s.effective_date_dt,
        s.discharge_date_dt,
        coalesce(s.effective_dept_id,0) as effective_dept_id,
        s.cancel_reason_cmt,
        s.bmi,
        s.avs_print_tm,
        s.avs_first_user_id,
        s.enc_close_time,
        s.ben_eng_sp_amt,
        s.entry_time, 
        regexp_replace(s.research_studycode, '[^0-9]+', '') as research_studycode,
        s.bill_num,
        s.es_audit_time,
        s.adt_pat_class_c,
        s.color_flag,
        s.pat_enc_date_real as enc_dt_real,
        coalesce(e.emp_key, -1) as cosign_emp_key,
        coalesce(p.prov_key, -1) as supervisor_prov_key,
        coalesce(sp.dim_supervisor_prov_type_key, -1) as dim_supervisor_prov_type_key,
        s.cosign_rev_ins_dttm as chart_cosign_dt,
        coalesce(visit_keys.ip_doc_contact_csn_visit_key, 0) as ip_documented_visit_key, 
        coalesce(visit_keys.visit_key, -1) as visit_key, 
        coalesce(dim_routed_msg_priority.dim_routed_msg_priority_key, -1) as dim_routed_msg_priority_key
    from
        {{ ref('stg_pat_enc') }} as s 
        left join visit_keys 
            on coalesce(s.pat_enc_csn_id,0)= visit_keys.pat_enc_csn_id 
        left join {{source('cdw','employee')}} as e 
            on coalesce(s.cosigner_user_id, '0') = e.emp_id 
            and e.comp_key = case when s.cosigner_user_id is null then 0 else 1 end
        left join {{source('cdw','provider')}} as p 
            on coalesce(s.sup_prov_id, '0') = p.prov_id 
        left join {{source('cdw','dim_supervisor_provider_type')}} as sp 
            on coalesce(s.sup_prov_c, '0') = sp.supervisor_prov_type_id
        left join {{source('cdw','dim_routed_msg_priority')}} as dim_routed_msg_priority 
            on coalesce(s.msg_priority_c, '0') = dim_routed_msg_priority.routed_msg_priority_id
        left join {{ref('stg_pat_enc_hsp')}} as stg_pat_enc_hsp
            on stg_pat_enc_hsp.pat_enc_csn_id = s.pat_enc_csn_id
),
ref_src_deduped as (
    select
        *,
        row_number() over (partition by ref_src_id order by rfl_prov_key desc) as dupe_ctr
    from
         {{ source('cdw', 'referral_source') }}
),
exp_all as (
    select
        s.visit_key,
        patient.pat_key,
        patient.pat_id,
        financial_class.fc_key,
        coalesce(provider_pcp.prov_id, '-1') as pc_prov_id,
        coalesce(provider_visit.prov_id, '-1') as visit_prov_id,
        s.dischrg_prov_id,
        coalesce(provider_pcp.prov_key, -1) as pcp_prov_key,
        coalesce(provider_visit.prov_key, -1) as visit_prov_key,
        coalesce(discharge_provider.prov_key, -1) as dischrg_prov_key,
        department.dept_key,
        effective_department.dept_key as eff_dept_key,
        procedure.proc_key,
        account.acct_key,
        coalesce(location.rev_loc_key, -1) as loc_key,
        benefit_plan.bp_key,
        payor.payor_key,
        payor.payor_id,
        program.prgm_key,
        service_area.svc_area_key,
        s.pat_enc_csn_id,
        master_date.dt_key as contact_dt_key,
        extract(epoch from s.effective_date_dt - patient.dob)/60/60/24/365.0 as age,
        s.bp_systolic,
        s.bp_diastolic,
        s.temperature,
        s.pulse,
        s.weight,
        s.weight * 0.0283495231 as weight_kg,
        s.height,
        replace(replace(s.height, chr(39), ''), '"', '') as height_format,
        substr(height_format, 0, instr(height_format, ' '))::decimal as height_ft,
        substr(height_format, instr(height_format, ' '))::decimal as height_in,
        ((height_ft * 12) + height_in) * 2.54 as height_cm,
        s.respirations,
        s.lmp_date,
        s.head_circumference,
        {{yn_to_ind('enc_closed_yn')}} as enc_closed_ind,
        s.enc_close_time,
        s.enc_dt_real::numeric(18,2) as enc_dt_real,
        s.appt_status_title as appt_status_c,
        s.appt_block_c,
        s.appt_time,
        s.checkin_time,
        s.checkout_time,
        s.hosp_admsn_time,
        s.hosp_dischrg_time,
        s.hosp_admsn_type_c,
        s.account_id,
        {{yn_to_ind('adm_for_surg_yn')}} as adm_for_surg_ind,
        {{yn_to_ind('is_walk_in_yn')}} as is_walk_in_ind,
        s.entity_c,
        s.effective_date_dt,
        s.discharge_date_dt,
        s.los_proc_code,
        extract(epoch from s.hosp_dischrg_time - s.hosp_admsn_time)/60/60.0 as los_hours,
        null::varchar(10) as display_age,
        s.appt_made_date,
        case 
            when s.enc_type_c = 50 then round(extract(epoch from s.contact_date - s.appt_made_date)/60.0/60/24)
            else -2
        end as appt_lag_days, 
        case when s.enc_type_c = 0 then -2 else coalesce(dict_ent_type.dict_key, -1) end as dict_enc_type_key,
        case when s.appt_status_c = 0 then -2 else coalesce(dict_appt_stat.dict_key, -1) end as dict_appt_stat_key,
        case 
            when s.enc_type_c = 50 and appt_lag_days <=30 then 251 
            when s.enc_type_c = 50 and appt_lag_days <=60 then 252
            when s.enc_type_c = 50 and appt_lag_days <=90 then 253 
            when s.enc_type_c = 50 and appt_lag_days <=120 then 254
            when s.enc_type_c = 50 and appt_lag_days <=150 then 255 
            when s.enc_type_c = 50 and appt_lag_days <=180 then 256 
            when s.enc_type_c = 50 and appt_lag_days <=365 then 257 
            when s.enc_type_c = 50 then 258
            else -2
        end as dict_appt_lag_bkt_key,
        s.hsp_account_id,
        extract(epoch from s.effective_date_dt - patient.dob)/60/60.0/24.0 as age_days,
        referral_source.ref_src_key,
        referral.rfl_key,
        {{yn_to_ind('referral_req_yn')}} as rfl_req_ind,
        current_date as etl_dt,
        'CLARITY' as etl_user,
        case when s.appt_prc_id is null then 0 else coalesce(master_visit_type.visit_type_key, -1) end as appt_visit_type_key,
        coalesce(s.copay_due, 0) as copay_due,
        coalesce(s.copay_collected, 0) as copay_collected,
        checkin_employee.emp_key as checkin_emp_key,
        coalesce(s.ben_eng_sp_amt, 0) as self_pay_amt,
        coalesce(appt_entry_employee.emp_key, -1) as appt_entry_emp_key,
        s.charge_slip_number,
        s.copay_ref_num,
        coalesce(dict_copay_source.dict_key, -2) as dict_copay_type_key,
        coalesce(coverage.cvg_key, -1) as cvg_key,
        coalesce(avs_employee.emp_key, -1) as avs_emp_key,
        s.avs_print_tm,
        s.appt_serial_no,
        -1 as hsp_acct_key,
        coalesce(research_study.res_stdy_key, 0) as res_stdy_key,
        coalesce(s.es_audit_time, s.appt_cancel_date) as appt_cancel_dt,
        case when extract(epoch from s.appt_time::date - s.appt_cancel_date)/60/60/24 <= 1 then 1 else 0 end as appt_cancel_24hr_ind,
        case when extract(epoch from s.appt_time::date - s.appt_cancel_date)/60/60/24 <= 2 then 1 else 0 end as appt_cancel_48hr_ind,
        s.bmi,
        s.bill_num,
        s.claim_id,
        s.appt_length,
        0 as dim_rsn_disch_key,
        coalesce(dim_phone_reminder_status.dim_phone_reminder_stat_key, 0) as dim_phone_reminder_stat_key, 
        dim_patient_class.dim_pat_class_key as dim_adt_pat_class_key,
        upper(s.color_flag) as visit_stat_color_cd,
        coalesce(dim_visit_cncl_rsn.dim_visit_cncl_rsn_key, -1) as dim_visit_cncl_rsn_key,
        coalesce(modifier_1.mod_key, -1) as level_svc_mod1_key,
        coalesce(modifier_2.mod_key, -1) as level_svc_mod2_key,
        coalesce(modifier_3.mod_key, -1) as level_svc_mod3_key,
        coalesce(modifier_4.mod_key, -1) as level_svc_mod4_key,
        s.entry_time,
        s.cosign_emp_key,
        s.supervisor_prov_key,
        s.dim_supervisor_prov_type_key,
        s.chart_cosign_dt,
        s.cancel_reason_cmt,
        s.ip_documented_visit_key,
        s.dim_routed_msg_priority_key
    from 
        sq_s_pat_enc s
        left join {{ source('cdw', 'procedure') }} as procedure on procedure.proc_id = s.los_prime_proc_id
        left join {{ source('cdw', 'account') }} as account on account.acct_id = s.account_id
        left join {{ source('cdw', 'provider') }} as provider_pcp on provider_pcp.prov_id = s.pc_prov_id
        left join {{ source('cdw', 'provider') }} as provider_visit on provider_visit.prov_id = s.visit_prov_id
        left join {{ source('cdw', 'provider') }} as discharge_provider on discharge_provider.prov_id = s.dischrg_prov_id
        left join {{ source('cdw', 'department') }} as location on location.dept_id = coalesce(s.department_id, 0)
        left join {{ source('cdw', 'referral') }} as referral on referral.rfl_id = s.referral_id
        left join ref_src_deduped as referral_source
            on referral_source.ref_src_id = s.referral_source_id
            and referral_source.dupe_ctr = 1
        left join {{ source('cdw', 'financial_class') }} as financial_class on financial_class.fc_id = s.visit_fc
        left join {{ source('cdw', 'master_date') }} as master_date on master_date.full_dt = s.contact_date
        left join {{ source('cdw', 'department') }} as department on department.dept_id = s.department_id
        left join {{ source('cdw', 'department') }} as effective_department on effective_department.dept_id = s.effective_dept_id
        left join {{ source('cdw', 'benefit_plan') }} as benefit_plan on benefit_plan.bp_id = s.visit_epp_id
        left join {{ source('cdw', 'payor') }} as payor on payor.payor_id = s.visit_epm_id
        left join {{ source('cdw', 'patient') }} as patient on patient.pat_id = s.pat_id
        left join {{ source('cdw', 'service_area') }} as service_area on service_area.svc_area_id = s.serv_area_id
        left join {{ source('cdw', 'program') }} as program on program.prgm_id = s.chkin_indicator_c /* convert to decimal? */
        left join {{ source('cdw', 'research_study') }} as research_study on research_study.res_stdy_id::varchar(50) = s.research_studycode
        left join {{ source('cdw', 'dim_visit_cncl_rsn') }} as dim_visit_cncl_rsn on dim_visit_cncl_rsn.visit_cncl_rsn_id = coalesce(s.cancel_reason_c::int, -2)
        left join {{ source('cdw', 'dim_phone_reminder_status') }} as dim_phone_reminder_status on dim_phone_reminder_status.phone_remind_stat_id = coalesce(s.phone_rem_stat_c, 0)
        left join {{ source('cdw', 'dim_patient_class') }} as dim_patient_class on dim_patient_class.pat_class_id = coalesce(s.adt_pat_class_c, '0')
        left join {{ source('cdw', 'master_visit_type') }} as master_visit_type on master_visit_type.visit_type_id = s.appt_prc_id        
        left join {{ source('cdw', 'master_modifier') }} as modifier_1 on modifier_1.mod_id = s.los_modifier1_id
        left join {{ source('cdw', 'master_modifier') }} as modifier_2 on modifier_2.mod_id = s.los_modifier2_id
        left join {{ source('cdw', 'master_modifier') }} as modifier_3 on modifier_3.mod_id = s.los_modifier3_id
        left join {{ source('cdw', 'master_modifier') }} as modifier_4 on modifier_4.mod_id = s.los_modifier4_id
        left join {{ source('cdw', 'employee') }} as checkin_employee
            on checkin_employee.emp_id = coalesce(s.checkin_user_id, '0')
            and checkin_employee.comp_key= (case when s.checkin_user_id is null then 0 else 1 end)
        left join {{ source('cdw', 'employee') }} as appt_entry_employee
            on appt_entry_employee.emp_id = coalesce(s.appt_entry_user_id, '0')
            and appt_entry_employee.comp_key= (case when s.appt_entry_user_id is null then 0 else 1 end)
        left join {{ source('cdw', 'employee') }} as avs_employee
            on avs_employee.emp_id = coalesce(s.avs_first_user_id, '0')
            and avs_employee.comp_key= (case when s.avs_first_user_id is null then 0 else 1 end)
        {{ join_cdw_dictionary('dict_ent_type', 's.enc_type_c', 5) }}
        {{ join_cdw_dictionary('dict_appt_stat', 's.appt_status_c', 6) }}
        left join {{ source('cdw', 'cdw_dictionary') }} as dict_copay_source
            on dict_copay_source.dict_cat_key = (case when s.copay_source_c is null then 0 else 10043 end)
            and dict_copay_source.dict_nm = s.copay_source_c
        left join {{ source('cdw', 'coverage') }} on coverage.cvg_id = coalesce(s.coverage_id, 0)
)
select
    exp_all.visit_key,
    exp_all.pat_key,
    exp_all.fc_key,
    exp_all.pcp_prov_key as pc_prov_key, /* Typo in actual visit table */
    exp_all.visit_prov_key,
    exp_all.dept_key,
    exp_all.eff_dept_key,
    exp_all.proc_key,
    exp_all.acct_key,
    exp_all.loc_key,
    exp_all.bp_key,
    exp_all.payor_key,
    exp_all.prgm_key,
    exp_all.svc_area_key,
    exp_all.contact_dt_key,
    exp_all.ref_src_key,
    exp_all.rfl_key,
    exp_all.appt_visit_type_key,
    exp_all.hsp_acct_key,
    exp_all.cvg_key,
    exp_all.avs_emp_key,
    exp_all.checkin_emp_key,
    exp_all.appt_entry_emp_key,
    coalesce(less_72_hour.less_72hr_visit_key, 0) as less_72hr_visit_key,
    exp_all.res_stdy_key,
    exp_all.cosign_emp_key,
    exp_all.supervisor_prov_key,
    exp_all.ip_documented_visit_key,
    exp_all.dict_enc_type_key,
    exp_all.dict_appt_stat_key,
    exp_all.dict_appt_lag_bkt_key,
    exp_all.dict_copay_type_key,
    last_stay_class.dict_visit_last_stay_cls_key,
    exp_all.dim_phone_reminder_stat_key,
    exp_all.dim_rsn_disch_key,
    exp_all.dim_adt_pat_class_key,
    exp_all.dim_visit_cncl_rsn_key,
    exp_all.dim_supervisor_prov_type_key,
    exp_all.dim_routed_msg_priority_key,
    exp_all.level_svc_mod1_key,
    exp_all.level_svc_mod2_key,
    exp_all.level_svc_mod3_key,
    exp_all.level_svc_mod4_key,
    exp_all.pat_enc_csn_id as enc_id,
    exp_all.claim_id,
    exp_all.age::numeric(9,4) as age,
    exp_all.bp_systolic as bp_sys,
    exp_all.bp_diastolic as bp_dias,
    cast(exp_all.temperature as numeric(7,3)) as temp,
    exp_all.pulse,
    exp_all.weight::numeric(10,4) as wt_oz,
    cast(exp_all.weight_kg as numeric(18,14)) as wt_kg,
    cast(exp_all.height as character varying(30)) as ht_raw,
    exp_all.height_cm::numeric(11,7) as ht_cm,
    exp_all.respirations,
    exp_all.head_circumference::numeric(7,3) as head_circ,
    exp_all.appt_status_c as appt_stat,
    exp_all.appt_block_c as appt_block,
    exp_all.hosp_admsn_type_c as hosp_admit_type,
    exp_all.entity_c::varchar(254) as entity,
    exp_all.los_hours as los_hours,
    exp_all.display_age as age_display,
    exp_all.appt_lag_days,
    exp_all.age_days,
    exp_all.copay_due,
    exp_all.copay_collected as copay_coll,
    exp_all.self_pay_amt,
    exp_all.charge_slip_number as chrg_slip_num,
    exp_all.copay_ref_num,
    exp_all.appt_serial_no as appt_sn,
    null::varchar(255) as contact_cmt,
    exp_all.enc_dt_real::numeric(18,2) as enc_dt_real,
    exp_all.appt_length as appt_lgth_min,
    exp_all.bmi as bmi,
    exp_all.bill_num as bill_nbr,
    cast(exp_all.cancel_reason_cmt as character varying(1200)) as cancel_reason_cmt,
    exp_all.los_proc_code as los_proc_cd,
    exp_all.visit_stat_color_cd,
    exp_all.effective_date_dt as eff_dt,
    exp_all.lmp_date as lmp_dt,
    exp_all.appt_time as appt_dt,
    exp_all.checkin_time as appt_checkin_dt,
    exp_all.checkout_time as appt_checkout_dt,
    exp_all.hosp_admsn_time as hosp_admit_dt,
    exp_all.hosp_dischrg_time as hosp_dischrg_dt,
    exp_all.discharge_date_dt as dischrg_dt,
    exp_all.appt_made_date as appt_made_dt,
    exp_all.avs_print_tm as avs_print_dt,
    exp_all.enc_close_time as enc_close_dt,
    exp_all.appt_cancel_dt,
    exp_all.entry_time as appt_entry_dt,
    exp_all.chart_cosign_dt,
    exp_all.enc_closed_ind,
    exp_all.adm_for_surg_ind,
    exp_all.is_walk_in_ind,
    exp_all.rfl_req_ind,
    coalesce(less_72_hour.less_72hr_hosp_admit_ind, 0) as less_72hr_hosp_admit_ind,
    exp_all.appt_cancel_24hr_ind,
    exp_all.appt_cancel_48hr_ind,
    last_stay_class.visit_last_stay_class_ind,
    cast(exp_all.pat_id as character varying(254)) as pat_id,
    cast(exp_all.visit_prov_id as character varying(254)) as visit_prov_id,
    cast(exp_all.pc_prov_id as character varying(254)) as pc_prov_id,
    cast(exp_all.dischrg_prov_id as character varying(254)) as dischrg_prov_id,
    cast(exp_all.payor_id as bigint) as payor_id,
    cast(exp_all.dischrg_prov_key as bigint) as dischrg_prov_key,
    current_timestamp as create_dt,
    'CLARITY' as create_by,
    current_timestamp as upd_dt,
    'CLARITY' as upd_by 
from 
    exp_all
    left join {{ ref('stg_visit_last_stay_class') }} as last_stay_class
        on last_stay_class.visit_key = exp_all.visit_key
    left join {{ ref('stg_visit_less_72_hour') }} as less_72_hour
        on less_72_hour.visit_key = exp_all.visit_key
