with visit_union_all_cte as (
    select *
    from
        {{ref('stg_visit_idx')}}
    union all
    select *
    from
        {{ref('stg_visit_idxrad')}}
    union all
    select *
    from
        {{ref('stg_visit_clarity')}}
     union all
    select *
    from
        {{ref('stg_visit_fastrack')}}
    union all
    select *
    from
        {{ref('stg_visit_admin')}}
)
select
    cast(coalesce(xwalk.legacy_visit_key, visit_all.visit_key) as bigint) as visit_key,
    -- cast(visit_key as bigint) as visit_key,
    cast(visit_all.pat_key as bigint) as pat_key,
    cast(fc_key as bigint) as fc_key,
    cast(pc_prov_key as bigint) as pc_prov_key,
    cast(visit_prov_key as bigint) as visit_prov_key,
    cast(dept_key as bigint) as dept_key,
    cast(eff_dept_key as bigint) as eff_dept_key,
    cast(proc_key as bigint) as proc_key,
    cast(acct_key as bigint) as acct_key,
    cast(loc_key as bigint) as loc_key,
    cast(bp_key as bigint) as bp_key,
    cast(payor_key as bigint) as payor_key,
    cast(prgm_key as bigint) as prgm_key,
    cast(svc_area_key as bigint) as svc_area_key,
    cast(coalesce(contact_dt_key, 99991231) as bigint) as contact_dt_key,
    cast(ref_src_key as bigint) as ref_src_key,
    cast(rfl_key as bigint) as rfl_key,
    cast(appt_visit_type_key as bigint) as appt_visit_type_key,
    cast(hsp_acct_key as bigint) as hsp_acct_key,
    cast(cvg_key as bigint) as cvg_key,
    cast(avs_emp_key as bigint) as avs_emp_key,
    cast(checkin_emp_key as bigint) as checkin_emp_key,
    cast(appt_entry_emp_key as bigint) as appt_entry_emp_key,
    cast(less_72hr_visit_key as bigint) as less_72hr_visit_key,
    cast(res_stdy_key as bigint) as res_stdy_key,
    cast(cosign_emp_key as bigint) as cosign_emp_key,
    cast(supervisor_prov_key as bigint) as supervisor_prov_key,
    cast(ip_documented_visit_key as bigint) as ip_documented_visit_key,
    cast(dict_enc_type_key as bigint) as dict_enc_type_key,
    cast(dict_appt_stat_key as bigint) as dict_appt_stat_key,
    cast(dict_appt_lag_bkt_key as bigint) as dict_appt_lag_bkt_key,
    cast(dict_copay_type_key as bigint) as dict_copay_type_key,
    cast(dict_visit_last_stay_cls_key as bigint) as dict_visit_last_stay_cls_key,
    cast(dim_phone_reminder_stat_key as integer) as dim_phone_reminder_stat_key,
    cast(dim_rsn_disch_key as bigint) as dim_rsn_disch_key,
    cast(dim_adt_pat_class_key as smallint) as dim_adt_pat_class_key,
    cast(dim_visit_cncl_rsn_key as integer) as dim_visit_cncl_rsn_key,
    cast(dim_supervisor_prov_type_key as integer) as dim_supervisor_prov_type_key,
    cast(dim_routed_msg_priority_key as integer) as dim_routed_msg_priority_key,
    cast(level_svc_mod1_key as bigint) as level_svc_mod1_key,
    cast(level_svc_mod2_key as bigint) as level_svc_mod2_key,
    cast(level_svc_mod3_key as bigint) as level_svc_mod3_key,
    cast(level_svc_mod4_key as bigint) as level_svc_mod4_key,
    cast(enc_id as numeric(14, 3)) as enc_id,
    cast(claim_id as bigint) as claim_id,
    cast(age as numeric(9, 4)) as age,
    cast(bp_sys as bigint) as bp_sys,
    cast(bp_dias as bigint) as bp_dias,
    cast(temp as numeric(7, 3)) as temp,
    cast(pulse as bigint) as pulse,
    cast(wt_oz as numeric(10, 4)) as wt_oz,
    cast(wt_kg as numeric(18, 14)) as wt_kg,
    cast(ht_raw as character varying(30)) as ht_raw,
    cast(ht_cm as numeric(11, 7)) as ht_cm,
    cast(respirations as bigint) as respirations,
    cast(head_circ as numeric(7, 3)) as head_circ,
    cast(appt_stat as character varying(254)) as appt_stat,
    cast(appt_block as character varying(254)) as appt_block,
    cast(hosp_admit_type as character varying(254)) as hosp_admit_type,
    cast(entity as character varying(254)) as entity,
    cast(los_hours as numeric(25, 18)) as los_hours,
    cast(age_display as character varying(50)) as age_display,
    cast(appt_lag_days as numeric(12, 2)) as appt_lag_days,
    cast(age_days as numeric(11, 4)) as age_days,
    cast(copay_due as numeric(12, 2)) as copay_due,
    cast(copay_coll as numeric(12, 2)) as copay_coll,
    cast(self_pay_amt as numeric(12, 2)) as self_pay_amt,
    cast(chrg_slip_num as character varying(20)) as chrg_slip_num,
    cast(copay_ref_num as character varying(300)) as copay_ref_num,
    cast(appt_sn as bigint) as appt_sn,
    cast(contact_cmt as character varying(255)) as contact_cmt,
    cast(enc_dt_real as double precision) as enc_dt_real,
    cast(appt_lgth_min as bigint) as appt_lgth_min,
    cast(bmi as numeric(18, 2)) as bmi,
    cast(bill_nbr as character varying(50)) as bill_nbr,
    cast(cancel_reason_cmt as character varying(1200)) as cancel_reason_cmt,
    cast(los_proc_cd as character varying(10)) as los_proc_cd,
    cast(visit_stat_color_cd as character varying(254)) as visit_stat_color_cd,
    cast(eff_dt as timestamp) as eff_dt,
    cast(lmp_dt as timestamp) as lmp_dt,
    cast(appt_dt as timestamp) as appt_dt,
    cast(appt_checkin_dt as timestamp) as appt_checkin_dt,
    cast(appt_checkout_dt as timestamp) as appt_checkout_dt,
    cast(hosp_admit_dt as timestamp) as hosp_admit_dt,
    cast(hosp_dischrg_dt as timestamp) as hosp_dischrg_dt,
    cast(dischrg_dt as timestamp) as dischrg_dt,
    cast(appt_made_dt as timestamp) as appt_made_dt,
    cast(avs_print_dt as timestamp) as avs_print_dt,
    cast(enc_close_dt as timestamp) as enc_close_dt,
    cast(appt_cancel_dt as timestamp) as appt_cancel_dt,
    cast(appt_entry_dt as timestamp) as appt_entry_dt,
    cast(chart_cosign_dt as timestamp) as chart_cosign_dt,
    cast(enc_closed_ind as byteint) as enc_closed_ind,
    cast(adm_for_surg_ind as byteint) as adm_for_surg_ind,
    cast(is_walk_in_ind as byteint) as is_walk_in_ind,
    cast(rfl_req_ind as byteint) as rfl_req_ind,
    cast(less_72hr_hosp_admit_ind as byteint) as less_72hr_hosp_admit_ind,
    cast(appt_cancel_24hr_ind as byteint) as appt_cancel_24hr_ind,
    cast(appt_cancel_48hr_ind as byteint) as appt_cancel_48hr_ind,
    cast(visit_last_stay_class_ind as byteint) as visit_last_stay_class_ind,
    cast(pat_id as character varying(254)) as pat_id,
    visit_prov_id,
    pc_prov_id,
    dischrg_prov_id,
    payor_id,
    dischrg_prov_key,
    cast(visit_all.create_dt as timestamp) as create_dt,
    cast(visit_all.create_by as character varying(20)) as create_by,
    cast(visit_all.upd_dt as timestamp) as upd_dt,
    cast(visit_all.upd_by as character varying(20)) as upd_by
from
    visit_union_all_cte as visit_all
    left join {{ source('manual_ods', 'xwalk_visit_key_cdw_to_dbt') }} as xwalk
        on xwalk.source_name = visit_all.create_by
        and xwalk.encounter_id = visit_all.enc_id
