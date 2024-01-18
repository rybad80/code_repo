select
    all_transactions.tdl_id,
    all_transactions.tx_id,
    all_transactions.chrg_tx_id,
    all_transactions.tx_num,
    all_transactions.detail_type,
    all_transactions.post_date,
    all_transactions.orig_post_date,
    all_transactions.chrg_post_date,
    all_transactions.orig_service_date,
    all_transactions.chrg_service_date,
    all_transactions.tran_type, -- 1=Charge; 2=Payment; 3=Adjustment
    all_transactions.match_trx_id,
    all_transactions.match_tx_type,
    clarity_eap_match.proc_name as match_proc_name,
    clarity_eap_match.proc_code as match_proc_code,
    all_transactions.match_proc_id,
    clarity_loc_match.loc_name as match_loc_name,
    all_transactions.match_loc_id,
    clarity_ser_match.prov_name as match_prov_name,
    all_transactions.match_prov_id,
    all_transactions.charge_slip_number,
    all_transactions.account_id,
    all_transactions.pat_id,
    patient.pat_mrn_id as mrn,
    all_transactions.amount,
    all_transactions.patient_amount,
    all_transactions.insurance_amount,
    all_transactions.user_id,
    all_transactions.debit_gl_num,
    all_transactions.credit_gl_num,
    clarity_ser_bill_prov.prov_name as billing_prov_name,
    all_transactions.billing_prov_id,
    clarity_ser_serv_prov.prov_name as servicing_prov_name,
    all_transactions.servicing_prov_id,
    case
        when all_transactions.detail_type in (1, 10)
            then COALESCE(arpb_transactions.coverage_id, -2)
        else null
	end as cur_cvg_id,
    case
        when all_transactions.detail_type in (1, 10)
			then
				case
					when coverage_last.plan_id is null then 'SELFPAY'
					else clarity_epp_cur.benefit_plan_name
				end
	end as cur_plan_name,
	case
        when all_transactions.detail_type in (1, 10)
			then COALESCE(coverage_last.plan_id, -2)
	end as cur_plan_id,
    case
        when all_transactions.detail_type in (1, 10)
			then
				case
					when arpb_transactions.payor_id is null then 'SELFPAY'
					else clarity_epm_cur.payor_name
				end
	end as cur_payor_name,
	case
        when all_transactions.detail_type in (1, 10)
			then COALESCE(arpb_transactions.payor_id, -2)
	end as cur_payor_id,
    case
        when all_transactions.detail_type in (1, 10)
			then
				case
					when CAST(clarity_fc_cur.internal_id as int) is null then 'Self-pay'
					else clarity_fc_cur.financial_class_name
				end
	end as cur_fin_class_name,
    case
        when all_transactions.detail_type in (1, 10)
			then
				case
					when CAST(clarity_fc_cur.internal_id as int) is null then 4
					else CAST(clarity_fc_cur.internal_id as int)
				end
	end as cur_fin_class,
    all_transactions.original_cvg_id,
    COALESCE(epp_original.benefit_plan_name, 'SELFPAY') as original_plan_name,
    all_transactions.original_plan_id,
    COALESCE(epm_original.payor_name, 'SELFPAY') as original_payor_name,
    all_transactions.original_payor_id,
    fc_original.financial_class_name as original_fin_class_name,
    all_transactions.original_fin_class,
    case
        when all_transactions.action_payor_id = -2 then 'SELFPAY'
        else clarity_epm_action.payor_name
    end as action_payor_name,
    all_transactions.action_payor_id,
    case
        when all_transactions.action_fin_class = 4 then 'Self-Pay'
        else clarity_fc_action.financial_class_name
    end as action_fin_class_name,
    all_transactions.action_fin_class,
    clarity_eap.proc_name as proc_name,
    clarity_eap.proc_code as proc_code,
    all_transactions.proc_id,
    case
        when clarity_eap_der.type_c = 1 then 'CHARGE'
        else clarity_epg_der.proc_group_name
    end as derived_proc_group,
    clarity_eap_der.proc_name as derived_proc_name,
    clarity_eap_der.proc_code as derived_proc_code,
    all_transactions.derived_proc_id,
    all_transactions.procedure_quantity,
    all_transactions.cpt_code,
    all_transactions.modifier_one,
    all_transactions.modifier_two,
    all_transactions.modifier_three,
    all_transactions.modifier_four,
    clarity_edg_dx1.dx_name as dx_one_name,
    clarity_edg_dx1.ref_bill_code as dx_one_icd,
    all_transactions.dx_one_id,
    clarity_edg_dx2.dx_name as dx_two_name,
    clarity_edg_dx2.ref_bill_code as dx_two_icd,
    all_transactions.dx_two_id,
    clarity_edg_dx3.dx_name as dx_three_name,
    clarity_edg_dx3.ref_bill_code as dx_three_icd,
    all_transactions.dx_three_id,
    clarity_edg_dx4.dx_name as dx_four_name,
    clarity_edg_dx4.ref_bill_code as dx_four_icd,
    all_transactions.dx_four_id,
    clarity_edg_dx5.dx_name as dx_five_name,
    clarity_edg_dx5.ref_bill_code as dx_five_icd,
    all_transactions.dx_five_id,
    clarity_edg_dx6.dx_name as dx_six_name,
    clarity_edg_dx6.ref_bill_code as dx_six_icd,
    all_transactions.dx_six_id,
    zc_loc_rpt_grp_6.name as location_grouper,
    clarity_loc.loc_name,
    all_transactions.loc_id,
    zc_dep_rpt_grp_10.name as division_name,
    clarity_dep.department_name,
    all_transactions.dept_id,
    clarity_pos.pos_name,
    all_transactions.pos_id,
    all_transactions.copay_indicator,
    all_transactions.referral_source_id,
    all_transactions.referral_id,
    all_transactions.posting_batch_num,
    all_transactions.base_units,
    all_transactions.timed_units, -- make sure these are calculated right - negative for detail type 10
    all_transactions.phy_status_units,
    all_transactions.emergency_units,
    all_transactions.age_units,
    all_transactions.caa_anes_case_count,
    all_transactions.caa_anes_units,
    all_transactions.visit_number,
    all_transactions.exp_vs_act_pmt_variance,
    all_transactions.allowed_amount,
    all_transactions.program_id,
    all_transactions.tx_comment,
    all_transactions.charge_amt,
    all_transactions.pmt_pract,
    all_transactions.pmt_gl,
    all_transactions.agg_bad_debt_adj_amount,
    all_transactions.agg_contract_adj_amount,
    all_transactions.agg_other_adj_amount,
    all_transactions.total_adj,
    all_transactions.orig_price,
    all_transactions.price_contract_id,
    all_transactions.contract_discount,
    all_transactions.base_work_rvu,
    all_transactions.base_overhead_rvu,
    all_transactions.base_malpractice_rvu,
    all_transactions.work_rvu,
    all_transactions.overhead_rvu,
    all_transactions.malpractice_rvu,
    all_transactions.anesthesia_rvu,
    all_transactions.total_rvu,
    all_transactions.pat_enc_csn_id,
    all_transactions.void_user_id,
    all_transactions.r_new_chg_tx_id,
    all_transactions.transferred_tx_id,
    all_transactions.is_tranfered_yn,
    all_transactions.reference_number,
    all_transactions.relative_value_unit,
    all_transactions.tdl_extract_date

from
    {{ref('stg_all_transactions')}} as all_transactions
    inner join
        {{ref('stg_arpb_transactions')}} as arpb_transactions on
            arpb_transactions.tx_id = all_transactions.tx_id
    left join
        {{ref('stg_coverage')}} as coverage_last on
            coverage_last.coverage_id = arpb_transactions.coverage_id
    left join
        {{source('clarity_ods', 'clarity_eap')}} as clarity_eap_match on
            clarity_eap_match.proc_id = all_transactions.match_proc_id
    left join
        {{source('clarity_ods', 'clarity_loc')}} as clarity_loc_match on
            clarity_loc_match.loc_id = all_transactions.match_loc_id
    left join
        {{source('clarity_ods', 'clarity_ser')}} as clarity_ser_match on
            clarity_ser_match.prov_id = all_transactions.match_prov_id
    left join
        {{source('clarity_ods', 'patient')}} as patient on
            patient.pat_id = all_transactions.pat_id
    left join
        {{source('clarity_ods', 'clarity_ser')}} as clarity_ser_bill_prov on
            clarity_ser_bill_prov.prov_id = all_transactions.billing_prov_id
    left join
        {{source('clarity_ods', 'clarity_ser')}} as clarity_ser_serv_prov on
            clarity_ser_serv_prov.prov_id = all_transactions.servicing_prov_id
    -- this gets current payor
    left join
        {{source('clarity_ods', 'clarity_epm')}} as epm_last on
            epm_last.payor_id = arpb_transactions.payor_id
    left join {{source('clarity_ods', 'clarity_fc')}} as fc_last on fc_last.internal_id = epm_last.financial_class
    left join
        {{source('clarity_ods', 'clarity_epp')}} as epp_original on
            epp_original.benefit_plan_id = all_transactions.original_plan_id
    left join
        {{source('clarity_ods', 'clarity_epm')}} as epm_original on
            epm_original.payor_id = all_transactions.original_payor_id
    left join
        {{source('clarity_ods', 'clarity_fc')}} as fc_original on
            fc_original.internal_id = all_transactions.original_fin_class
    left join
        {{source('clarity_ods', 'clarity_eap')}} as clarity_eap on
            clarity_eap.proc_id = all_transactions.proc_id
    left join
        {{source('clarity_ods', 'clarity_loc')}} as clarity_loc
            on clarity_loc.loc_id = all_transactions.loc_id
    left join
        {{source('clarity_ods', 'zc_loc_rpt_grp_6')}} as zc_loc_rpt_grp_6 on
            zc_loc_rpt_grp_6.internal_id = clarity_loc.rpt_grp_six
    left join
        {{source('clarity_ods', 'clarity_dep')}} as clarity_dep on
            clarity_dep.department_id = all_transactions.dept_id
    left join
        {{source('clarity_ods', 'zc_dep_rpt_grp_10')}} as zc_dep_rpt_grp_10 on
            zc_dep_rpt_grp_10.internal_id = clarity_dep.rpt_grp_ten
    left join
        {{source('clarity_ods', 'clarity_pos')}} as clarity_pos on
            clarity_pos.pos_id = all_transactions.pos_id
    left join
        {{source('clarity_ods', 'clarity_edg')}} as clarity_edg_dx1 on
            clarity_edg_dx1.dx_id = all_transactions.dx_one_id
    left join
        {{source('clarity_ods', 'clarity_edg')}} as clarity_edg_dx2 on
            clarity_edg_dx2.dx_id = all_transactions.dx_two_id
    left join
        {{source('clarity_ods', 'clarity_edg')}} as clarity_edg_dx3 on
            clarity_edg_dx3.dx_id = all_transactions.dx_three_id
    left join
        {{source('clarity_ods', 'clarity_edg')}} as clarity_edg_dx4 on
            clarity_edg_dx4.dx_id = all_transactions.dx_four_id
    left join
        {{source('clarity_ods', 'clarity_edg')}} as clarity_edg_dx5 on
            clarity_edg_dx5.dx_id = all_transactions.dx_five_id
    left join
        {{source('clarity_ods', 'clarity_edg')}} as clarity_edg_dx6 on
            clarity_edg_dx6.dx_id = all_transactions.dx_six_id
    -- CASE statement fields
    left join
        {{source('clarity_ods', 'clarity_epm')}} as clarity_epm_cur on
            clarity_epm_cur.payor_id = arpb_transactions.payor_id
    left join
        {{source('clarity_ods', 'clarity_fc')}} as clarity_fc_cur on
            clarity_fc_cur.internal_id = clarity_epm_cur.financial_class
    left join
        {{source('clarity_ods', 'clarity_epp')}} as clarity_epp_cur on
            clarity_epp_cur.benefit_plan_id = coverage_last.plan_id
    left join
        {{source('clarity_ods', 'clarity_epm')}} as clarity_epm_action on
            clarity_epm_action.payor_id = all_transactions.action_payor_id
    left join
        {{source('clarity_ods', 'clarity_fc')}} as clarity_fc_action on
            clarity_fc_action.internal_id = all_transactions.action_fin_class
    -- Derived Procedure fields
    left join
        {{source('clarity_ods', 'clarity_eap')}} as clarity_eap_der on
            clarity_eap_der.proc_id = all_transactions.derived_proc_id
    left join
        {{source('clarity_ods', 'clarity_epg')}} as clarity_epg_der on
            clarity_epg_der.proc_group_id = clarity_eap_der.proc_group_id
