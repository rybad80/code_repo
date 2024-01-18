/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_cdw_stage",
  "WORKLET_NAME": "wklt_stg_8",
  "SESSION_NAME": "s_stg_load_pat_enc_hsp",
  "MAPPING_NAME": "m_stg_load_cl_pat_enc_hsp",
  "MAPPING_ID": 7285,
  "TARGET_ID": 7310,
  "TARGET_NAME": "s_cl_pat_enc_hsp"
}
*/
{{ config(dist='pat_enc_csn_id') }}

with sq_pat_enc_hsp as (
    with edecu_admit_rsn as (
        --- added max order_proc_id to remove duplicates
        select
            op.pat_enc_csn_id,
            osq.ord_quest_resp as edecu_admit_rsn,
            max(op.order_proc_id) as order_proc_id
        from {{ source('clarity_ods', 'ord_spec_quest') }} as osq
        inner join {{ source('clarity_ods', 'order_proc') }} as op
            on osq.order_id = op.order_proc_id
        inner join
            (
                select
                    op.pat_enc_csn_id,
                    min(op.instantiated_time) as min_instantiated_time
                from {{ source('clarity_ods', 'order_proc') }} as op
                where op.proc_id = 83997
                and op.order_status_c != 4
                group by op.pat_enc_csn_id
            ) as x
            on
                op.pat_enc_csn_id = x.pat_enc_csn_id
                and op.instantiated_time = x.min_instantiated_time
        where
            exists (
                select 1
                from {{ source('clarity_ods', 'pat_enc_hsp') }} as hsp
                where hsp.pat_enc_csn_id = op.pat_enc_csn_id
            )
            and osq.ord_quest_id = '500500659'
            and op.proc_id = 83997
            and op.order_status_c != 4
            and lower(osq.ord_quest_resp) like 'ed%'
        group by op.pat_enc_csn_id, osq.ord_quest_resp
    ),

    edecu as (
        select distinct * from (
            select
                clarity_adt.pat_enc_csn_id,
                clarity_adt.effective_time,
                min(clarity_adt.effective_time) over (
                    partition by clarity_adt.pat_enc_csn_id
                    order by seq_num_in_enc desc
                ) as min_time,
                min(clarity_rom.room_name) over (
                    partition by clarity_adt.pat_enc_csn_id
                    order by seq_num_in_enc desc
                ) as room_name
            from (
                select
                    x.pat_enc_csn_id,
                    max(x.seq_num_in_enc) as max_seq_num_in_enc
                from {{ source('clarity_ods', 'clarity_adt') }} as x
                where
                    x.event_type_c = 3
                    and x.event_subtype_c != 2
                    and x.department_id = 10201512
                group by x.pat_enc_csn_id
            ) as x
            inner join
                {{ source('clarity_ods', 'clarity_adt') }} as clarity_adt
                on clarity_adt.pat_enc_csn_id = x.pat_enc_csn_id
                and clarity_adt.seq_num_in_enc = x.max_seq_num_in_enc
            left join {{ source('clarity_ods', 'clarity_rom') }} as clarity_rom
                on clarity_adt.room_csn_id = clarity_rom.room_csn_id
            where
                event_type_c = 3
                and clarity_adt.event_subtype_c != 2
                and clarity_adt.department_id = 10201512
        ) as tmp1
        where
            tmp1.effective_time = tmp1.min_time
            and exists (
                select 1
                from {{ source('clarity_ods', 'pat_enc_hsp') }} as hsp
                where hsp.pat_enc_csn_id = tmp1.pat_enc_csn_id
            )
    ),

    x_cuml_room_nm as (
        select distinct
            x.pat_enc_csn_id,
            --as room1
            max(case when x.line_rank = 1 then x.room_name end) over (
                partition by x.pat_enc_csn_id
            )
            --as room2
            || max(
                case when x.line_rank = 2 then '; ' || x.room_name end
            ) over (partition by x.pat_enc_csn_id)
            --as room3
            || max(
                case when x.line_rank = 3 then '; ' || x.room_name end
            ) over (partition by x.pat_enc_csn_id)
            -- as room4
            || max(
                case when x.line_rank = 4 then '; ' || x.room_name end
            ) over (partition by x.pat_enc_csn_id)
            --as room5
            || max(
                case when x.line_rank = 5 then '; ' || x.room_name end
            ) over (partition by x.pat_enc_csn_id)
            || max(
                case when x.line_rank = 6 then '; ' || x.room_name end
            ) over (partition by x.pat_enc_csn_id) as cuml_room_nm
        from (
            select distinct
                pat_enc_csn_id,
                clarity_rom.room_name,
                dense_rank() over (
                    partition by pat_enc_csn_id order by effective_time
                ) as line_rank
            from {{ source('clarity_ods', 'clarity_adt') }} as clarity_adt
            left join {{ source('clarity_ods', 'clarity_rom') }} as clarity_rom
                on clarity_adt.room_csn_id = clarity_rom.room_csn_id
            where
                event_type_c in (1, 3)
                and event_subtype_c != 2
                and room_name is not null
                and exists (
                    select 1
                    from {{ source('clarity_ods', 'pat_enc_hsp') }} as hsp
                    where hsp.pat_enc_csn_id = clarity_adt.pat_enc_csn_id
                )
        ) as x
    ),

    atnd_spec as (
        select
            atnd.hsp_account_id,
            ser.prov_id,
            ser.prov_name,
            ser.referral_source_type,
            upper(zc.name) as atnd_specialty
        from {{ source('clarity_ods', 'hsp_acct_atnd_prov') }} as atnd
        left join {{ source('clarity_ods', 'clarity_ser') }} as ser
            on ser.prov_id = atnd.attending_prov_id
        left join {{ source('clarity_ods', 'clarity_ser_spec') }} as spec
            on spec.prov_id = ser.prov_id and spec.line = 1
        left join {{ source('clarity_ods', 'zc_specialty') }} as zc
            on zc.specialty_c = spec.specialty_c
        where atnd.line = 1
    )
    select
        peh.pat_id,
        peh.pat_enc_date_real,
        peh.pat_enc_csn_id,
        peh.adt_contact,
        peh.adt_initial,
        {{clean_id('peh', 'adt_pat_class_c')}},
        peh.adt_billing_type_c,
        peh.adt_patient_stat_c,
        peh.level_of_care_c,
        peh.pending_disch_time,
        peh.disch_code_c,
        peh.adt_athcrt_stat_c,
        peh.adt_last_rvw_dt,
        peh.adt_next_rvw_dt,
        peh.preadm_undo_rsn_c,
        peh.exp_admission_time,
        peh.exp_len_of_stay,
        peh.exp_discharge_date,
        peh.admit_category_c,
        peh.admit_source_c,
        peh.type_of_room_c,
        peh.rsn_for_room_c,
        peh.type_of_bed_c,
        peh.rsn_for_bed_c,
        peh.belong_claim_no,
        peh.belong_recv_time,
        peh.belong_recv_pers,
        peh.belong_location,
        peh.delivery_type_c,
        peh.labor_status_c,
        peh.er_injury,
        peh.adt_arrival_time,
        peh.adt_arrival_sts_c,
        peh.hosp_admsn_time,
        peh.admit_conf_stat_c,
        peh.hosp_disch_time,
        peh.disch_conf_stat_c,
        peh.discharge_prov_id,
        peh.admission_prov_id,
        {{clean_id('peh','hosp_admsn_type_c')}},
        peh.department_id,
        peh.adt_serv_area_id,
        peh.room_id,
        peh.bed_id,
        {{clean_id('peh','hosp_serv_c')}}, 
        peh.means_of_depart_c,
        {{clean_id('peh','disch_disp_c')}}, 
        {{clean_id('peh','disch_dest_c')}},
        peh.transfer_from_c,
        null as pat_contact_mpi_no,
        /*  the column PAT_CONTACT_MPI_NO has been deprecated.
        The patient's MRN, of the type associated with the location for this contact.
        The MRN displayed is not guaranteed to be current. eplacement Columns:
        IDENTITY_ID.IDENTITY_ID, PATIENT.PAT_MRN_ID, V_PAT_EAF_MRN.PAT_MRN
        */
        peh.hsp_account_id,
        {{clean_id('peh', 'means_of_arrv_c')}},
        peh.bill_num_type_c,
        peh.bill_num,
        peh.relig_affil_yn,
        {{clean_id('peh', 'acuity_level_c')}},
        peh.pat_escorted_by_c,
        peh.hospist_needed_yn,
        peh.accommodation_c,
        peh.accom_reason_c,
        peh.adm_event_id,
        peh.dis_event_id,
        peh.inpatient_data_id,
        peh.ip_episode_id,
        peh.pvt_hsp_enc_c,
        peh.contact_date,
        peh.ed_episode_id,
        {{clean_id('peh', 'ed_disposition_c')}},
        peh.ed_disp_time,
        peh.followup_prov_id,
        peh.prov_cont_info,
        peh.ed_area_of_care_id,
        peh.cm_ct_owner_id,
        peh.oshpd_admsn_src_c,
        peh.oshpd_licensure_c,
        peh.oshpd_route_c,
        peh.inp_adm_date,
        peh.copy_to_pcp_yn,
        peh.adoption_case_yn,
        peh.preop_teaching_c,
        peh.preop_prn_eval_c,
        peh.preop_ph_screen_c,
        peh.labor_act_birth_c,
        peh.labor_feed_type_c,
        peh.er_badge_number,
        peh.proc_serv_c,
        peh.cancel_user_id,
        peh.inp_adm_event_id,
        peh.inp_adm_event_date,
        peh.inp_dwngrd_evnt_id,
        peh.inp_dwngrd_date,
        peh.inp_dwngrd_evnt_dt,
        peh.ed_departure_time,
        peh.triage_datetime,
        peh.triage_status_c,
        peh.belong_rel_pers,
        peh.belong_rel_date,
        peh.belong_store_loc_c,
        peh.eddisp_edit_user_id,
        peh.eddisp_edit_inst,
        peh.op_adm_date,
        peh.emer_adm_date,
        peh.op_adm_event_id,
        peh.emer_adm_event_id,
        peh.prereg_source_c,
        peh.exp_discharge_time,
        peh.discharge_cat_c,
        peh.instant_of_entry_tm,
        peh.hov_conf_status_c,
        peh.relig_needs_visit_c,
        peh.bill_attend_prov_id,
        peh.ob_ld_laboring_yn,
        peh.ob_ld_labor_tm,
        peh.triage_id_tag,
        peh.ed_fu_edit_user_id,
        peh.ed_fu_edit_inst,
        peh.triage_id_tag_cmt,
        peh.tplnt_bill_stat_c,
        peh.need_fin_clr_yn,
        e2.effective_time as edecu_arrival,
        e.edecu_admit_rsn as edecu_reason_to_admit,
        e2.room_name as edecu_room,
        rm.cuml_room_nm,
        peh.referring_dept_id,
        peh.actl_delivry_meth_c,
        peh.prenatal_care_c,
        peh.ambulance_code_c,
        peh.mse_date,
        peh.admit_prov_text,
        peh.attend_prov_text,
        peh.prov_prim_text,
        peh.prov_prim_text_phon,
        peh.hospital_area_id,
        peh.admit_addr_id,
        peh.chief_complaint_c,
        peh.mu_hosp_admsn_time,
        case
            when trim(srvc.name) is not null then upper(srvc.name)
            when
                trim(atnd_spec.atnd_specialty) is not null
                then upper(atnd_spec.atnd_specialty)
            else upper(dep_spec.specialty)
        end as derived_hosp_srvc
    from {{ source('clarity_ods', 'pat_enc_hsp') }} as peh
    inner join {{ source('clarity_ods', 'pat_enc') }} as pat_enc
        on peh.pat_enc_csn_id = pat_enc.pat_enc_csn_id
    left join edecu_admit_rsn as e
        on e.pat_enc_csn_id = peh.pat_enc_csn_id
    left join edecu as e2
        on e2.pat_enc_csn_id = peh.pat_enc_csn_id
    left join x_cuml_room_nm as rm
        on rm.pat_enc_csn_id = peh.pat_enc_csn_id
    left join {{ source('clarity_ods', 'zc_pat_service') }} as srvc
         on srvc.hosp_serv_c = peh.hosp_serv_c
    left join {{ source('clarity_ods', 'hsp_account') }} as hsp
        on hsp.hsp_account_id = peh.hsp_account_id
    left join {{ source('clarity_ods', 'clarity_dep') }} as dep_spec
        on dep_spec.department_id = hsp.disch_dept_id
    left join {{ source('clarity_ods', 'atnd_spec') }} as atnd_spec
        on atnd_spec.hsp_account_id = peh.hsp_account_id

),
s_cl_pat_enc_hsp as (
    select
        cast(accommodation_c as bigint) as accommodation_c,
        cast(accom_reason_c as bigint) as accom_reason_c,
        cast(actl_delivry_meth_c as varchar(66)) as actl_delivry_meth_c,
        cast(acuity_level_c as bigint) as acuity_level_c,
        cast(admission_prov_id as varchar(18)) as admission_prov_id,
        cast(admit_addr_id as varchar(100)) as admit_addr_id,
        cast(admit_category_c as bigint) as admit_category_c,
        cast(admit_conf_stat_c as bigint) as admit_conf_stat_c,
        cast(admit_prov_text as varchar(254)) as admit_prov_text,
        cast(admit_source_c as varchar(254)) as admit_source_c,
        cast(adm_event_id as bigint) as adm_event_id,
        cast(adoption_case_yn as char) as adoption_case_yn,
        cast(adt_arrival_sts_c as bigint) as adt_arrival_sts_c,
        cast(adt_arrival_time as timestamp) as adt_arrival_time,
        cast(adt_athcrt_stat_c as bigint) as adt_athcrt_stat_c,
        cast(adt_billing_type_c as bigint) as adt_billing_type_c,
        cast(adt_contact as bigint) as adt_contact,
        cast(adt_initial as varchar(12)) as adt_initial,
        cast(adt_last_rvw_dt as timestamp) as adt_last_rvw_dt,
        cast(adt_next_rvw_dt as timestamp) as adt_next_rvw_dt,
        cast(adt_patient_stat_c as bigint) as adt_patient_stat_c,
        cast(adt_pat_class_c as bigint) as adt_pat_class_c,
        cast(adt_serv_area_id as bigint) as adt_serv_area_id,
        cast(ambulance_code_c as bigint) as ambulance_code_c,
        cast(attend_prov_text as varchar(254)) as attend_prov_text,
        cast(bed_id as varchar(18)) as bed_id,
        cast(belong_claim_no as varchar(80)) as belong_claim_no,
        cast(belong_location as varchar(255)) as belong_location,
        cast(belong_recv_pers as varchar(80)) as belong_recv_pers,
        cast(belong_recv_time as timestamp) as belong_recv_time,
        cast(belong_rel_date as timestamp) as belong_rel_date,
        cast(belong_rel_pers as varchar(80)) as belong_rel_pers,
        cast(belong_store_loc_c as bigint) as belong_store_loc_c,
        cast(bill_attend_prov_id as varchar(18)) as bill_attend_prov_id,
        cast(bill_num as varchar(50)) as bill_num,
        cast(bill_num_type_c as bigint) as bill_num_type_c,
        cast(cancel_user_id as varchar(18)) as cancel_user_id,
        cast(chief_complaint_c as varchar(66)) as chief_complaint_c,
        cast(cm_ct_owner_id as varchar(25)) as cm_ct_owner_id,
        cast(contact_date as timestamp) as contact_date,
        cast(copy_to_pcp_yn as char) as copy_to_pcp_yn,
        cast(cuml_room_nm as varchar(1000)) as cuml_room_nm,
        cast(delivery_type_c as varchar(66)) as delivery_type_c,
        cast(department_id as bigint) as department_id,
        cast(derived_hosp_srvc as varchar(500)) as derived_hosp_srvc,
        cast(discharge_cat_c as bigint) as discharge_cat_c,
        cast(discharge_prov_id as varchar(18)) as discharge_prov_id,
        cast(disch_code_c as bigint) as disch_code_c,
        cast(disch_conf_stat_c as bigint) as disch_conf_stat_c,
        cast(disch_dest_c as bigint) as disch_dest_c,
        cast(disch_disp_c as bigint) as disch_disp_c,
        cast(dis_event_id as bigint) as dis_event_id,
        cast(eddisp_edit_inst as timestamp) as eddisp_edit_inst,
        cast(eddisp_edit_user_id as varchar(18)) as eddisp_edit_user_id,
        cast(edecu_arrival as timestamp) as edecu_arrival,
        cast(edecu_reason_to_admit as varchar(1000)) as edecu_reason_to_admit,
        cast(edecu_room as varchar(50)) as edecu_room,
        cast(ed_area_of_care_id as bigint) as ed_area_of_care_id,
        cast(ed_departure_time as timestamp) as ed_departure_time,
        cast(ed_disposition_c as bigint) as ed_disposition_c,
        cast(ed_disp_time as timestamp) as ed_disp_time,
        cast(ed_episode_id as bigint) as ed_episode_id,
        cast(ed_fu_edit_inst as timestamp) as ed_fu_edit_inst,
        cast(ed_fu_edit_user_id as varchar(18)) as ed_fu_edit_user_id,
        cast(emer_adm_date as timestamp) as emer_adm_date,
        cast(emer_adm_event_id as bigint) as emer_adm_event_id,
        cast(er_badge_number as varchar(50)) as er_badge_number,
        cast(er_injury as varchar(255)) as er_injury,
        cast(exp_admission_time as timestamp) as exp_admission_time,
        cast(exp_discharge_date as timestamp) as exp_discharge_date,
        cast(exp_discharge_time as timestamp) as exp_discharge_time,
        cast(exp_len_of_stay as bigint) as exp_len_of_stay,
        cast(followup_prov_id as varchar(18)) as followup_prov_id,
        cast(hospist_needed_yn as char) as hospist_needed_yn,
        cast(hospital_area_id as bigint) as hospital_area_id,
        cast(hosp_admsn_time as timestamp) as hosp_admsn_time,
        cast(hosp_admsn_type_c as bigint) as hosp_admsn_type_c,
        cast(hosp_disch_time as timestamp) as hosp_disch_time,
        cast(hosp_serv_c as bigint) as hosp_serv_c,
        cast(hov_conf_status_c as bigint) as hov_conf_status_c,
        cast(hsp_account_id as bigint) as hsp_account_id,
        cast(inpatient_data_id as varchar(18)) as inpatient_data_id,
        cast(inp_adm_date as timestamp) as inp_adm_date,
        cast(inp_adm_event_date as timestamp) as inp_adm_event_date,
        cast(inp_adm_event_id as bigint) as inp_adm_event_id,
        cast(inp_dwngrd_date as timestamp) as inp_dwngrd_date,
        cast(inp_dwngrd_evnt_dt as timestamp) as inp_dwngrd_evnt_dt,
        cast(inp_dwngrd_evnt_id as bigint) as inp_dwngrd_evnt_id,
        cast(instant_of_entry_tm as timestamp) as instant_of_entry_tm,
        cast(ip_episode_id as bigint) as ip_episode_id,
        cast(labor_act_birth_c as bigint) as labor_act_birth_c,
        cast(labor_feed_type_c as bigint) as labor_feed_type_c,
        cast(labor_status_c as bigint) as labor_status_c,
        cast(level_of_care_c as varchar(66)) as level_of_care_c,
        cast(means_of_arrv_c as bigint) as means_of_arrv_c,
        cast(means_of_depart_c as bigint) as means_of_depart_c,
        cast(mse_date as timestamp) as mse_date,
        cast(mu_hosp_admsn_time as timestamp) as mu_hosp_admsn_time,
        cast(need_fin_clr_yn as char) as need_fin_clr_yn,
        cast(ob_ld_laboring_yn as char) as ob_ld_laboring_yn,
        cast(ob_ld_labor_tm as timestamp) as ob_ld_labor_tm,
        cast(op_adm_date as timestamp) as op_adm_date,
        cast(op_adm_event_id as bigint) as op_adm_event_id,
        cast(oshpd_admsn_src_c as bigint) as oshpd_admsn_src_c,
        cast(oshpd_licensure_c as bigint) as oshpd_licensure_c,
        cast(oshpd_route_c as bigint) as oshpd_route_c,
        cast(pat_contact_mpi_no as varchar(25)) as pat_contact_mpi_no,
        cast(pat_enc_csn_id as bigint) as pat_enc_csn_id,
        cast(pat_enc_date_real as double) as pat_enc_date_real,
        cast(pat_escorted_by_c as bigint) as pat_escorted_by_c,
        cast(pat_id as varchar(18)) as pat_id,
        cast(pending_disch_time as timestamp) as pending_disch_time,
        cast(preadm_undo_rsn_c as bigint) as preadm_undo_rsn_c,
        cast(prenatal_care_c as bigint) as prenatal_care_c,
        cast(preop_ph_screen_c as bigint) as preop_ph_screen_c,
        cast(preop_prn_eval_c as bigint) as preop_prn_eval_c,
        cast(preop_teaching_c as bigint) as preop_teaching_c,
        cast(prereg_source_c as bigint) as prereg_source_c,
        cast(proc_serv_c as bigint) as proc_serv_c,
        cast(prov_cont_info as varchar(254)) as prov_cont_info,
        cast(prov_prim_text as varchar(254)) as prov_prim_text,
        cast(prov_prim_text_phon as varchar(254)) as prov_prim_text_phon,
        cast(pvt_hsp_enc_c as bigint) as pvt_hsp_enc_c,
        cast(referring_dept_id as bigint) as referring_dept_id,
        cast(relig_affil_yn as char) as relig_affil_yn,
        cast(relig_needs_visit_c as bigint) as relig_needs_visit_c,
        cast(room_id as varchar(18)) as room_id,
        cast(rsn_for_bed_c as varchar(66)) as rsn_for_bed_c,
        cast(rsn_for_room_c as bigint) as rsn_for_room_c,
        cast(tplnt_bill_stat_c as bigint) as tplnt_bill_stat_c,
        cast(transfer_from_c as bigint) as transfer_from_c,
        cast(triage_datetime as timestamp) as triage_datetime,
        cast(triage_id_tag as varchar(184)) as triage_id_tag,
        cast(triage_id_tag_cmt as varchar(184)) as triage_id_tag_cmt,
        cast(triage_status_c as bigint) as triage_status_c,
        cast(type_of_bed_c as varchar(66)) as type_of_bed_c,
        cast(type_of_room_c as bigint) as type_of_room_c
    from sq_pat_enc_hsp
)
select
    pat_id,
    pat_enc_date_real,
    pat_enc_csn_id,
    adt_contact,
    adt_initial,
    adt_pat_class_c,
    adt_billing_type_c,
    adt_patient_stat_c,
    level_of_care_c,
    pending_disch_time,
    disch_code_c,
    adt_athcrt_stat_c,
    adt_last_rvw_dt,
    adt_next_rvw_dt,
    preadm_undo_rsn_c,
    exp_admission_time,
    exp_len_of_stay,
    exp_discharge_date,
    admit_category_c,
    admit_source_c,
    type_of_room_c,
    rsn_for_room_c,
    type_of_bed_c,
    rsn_for_bed_c,
    belong_claim_no,
    belong_recv_time,
    belong_recv_pers,
    belong_location,
    delivery_type_c,
    labor_status_c,
    er_injury,
    adt_arrival_time,
    adt_arrival_sts_c,
    hosp_admsn_time,
    admit_conf_stat_c,
    hosp_disch_time,
    disch_conf_stat_c,
    discharge_prov_id,
    admission_prov_id,
    hosp_admsn_type_c,
    department_id,
    adt_serv_area_id,
    room_id,
    bed_id,
    hosp_serv_c,
    means_of_depart_c,
    disch_disp_c,
    disch_dest_c,
    transfer_from_c,
    pat_contact_mpi_no,
    hsp_account_id,
    means_of_arrv_c,
    bill_num_type_c,
    bill_num,
    relig_affil_yn,
    acuity_level_c,
    pat_escorted_by_c,
    hospist_needed_yn,
    accommodation_c,
    accom_reason_c,
    adm_event_id,
    dis_event_id,
    inpatient_data_id,
    ip_episode_id,
    pvt_hsp_enc_c,
    contact_date,
    ed_episode_id,
    ed_disposition_c,
    ed_disp_time,
    followup_prov_id,
    prov_cont_info,
    ed_area_of_care_id,
    cm_ct_owner_id,
    oshpd_admsn_src_c,
    oshpd_licensure_c,
    oshpd_route_c,
    inp_adm_date,
    copy_to_pcp_yn,
    adoption_case_yn,
    preop_teaching_c,
    preop_prn_eval_c,
    preop_ph_screen_c,
    labor_act_birth_c,
    labor_feed_type_c,
    er_badge_number,
    proc_serv_c,
    cancel_user_id,
    inp_adm_event_id,
    inp_adm_event_date,
    inp_dwngrd_evnt_id,
    inp_dwngrd_date,
    inp_dwngrd_evnt_dt,
    ed_departure_time,
    triage_datetime,
    triage_status_c,
    belong_rel_pers,
    belong_rel_date,
    belong_store_loc_c,
    eddisp_edit_user_id,
    eddisp_edit_inst,
    op_adm_date,
    emer_adm_date,
    op_adm_event_id,
    emer_adm_event_id,
    prereg_source_c,
    exp_discharge_time,
    discharge_cat_c,
    instant_of_entry_tm,
    hov_conf_status_c,
    relig_needs_visit_c,
    bill_attend_prov_id,
    ob_ld_laboring_yn,
    ob_ld_labor_tm,
    triage_id_tag,
    ed_fu_edit_user_id,
    ed_fu_edit_inst,
    triage_id_tag_cmt,
    tplnt_bill_stat_c,
    need_fin_clr_yn,
    edecu_arrival,
    edecu_reason_to_admit,
    edecu_room,
    cuml_room_nm,
    derived_hosp_srvc,
    referring_dept_id,
    actl_delivry_meth_c,
    prenatal_care_c,
    ambulance_code_c,
    mse_date,
    admit_prov_text,
    attend_prov_text,
    prov_prim_text,
    prov_prim_text_phon,
    hospital_area_id,
    admit_addr_id,
    chief_complaint_c,
    mu_hosp_admsn_time
from s_cl_pat_enc_hsp
