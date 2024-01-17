with
open_ar as (
    select
        arpb_transactions.tx_id,
        clarity_tdl_tran.tx_num,
        arpb_transactions.post_date,
        arpb_transactions.service_date,
        arpb_transactions.account_id,
        arpb_transactions.patient_id,
        arpb_transactions.amount,
        arpb_transactions.patient_amt as pat_outstanding_amt,
        arpb_transactions.insurance_amt as ins_outstanding_amt,
        arpb_transactions.outstanding_amt as total_outstanding_amt,
        clarity_tdl_tran.allowed_amount,
        arpb_transactions.total_match_amt,
        arpb_transactions.total_mtch_ins_adj,
        arpb_transactions.total_mtch_ins_amt,
        arpb_transactions.total_mtch_adj,
        arpb_transactions.procedure_quantity,
        arpb_transactions.zero_balance_yn,
        arpb_transactions.user_id,
        clarity_ser_bill_prov.prov_name as billing_prov_name,
        clarity_ser_2.npi as billing_prov_npi,
        arpb_transactions.billing_prov_id,
        clarity_ser_serv_prov.prov_name as servicing_prov_name,
        arpb_transactions.serv_provider_id as servicing_prov_id,
        arpb_transactions.coverage_id as cur_cvg_id,
        clarity_tdl_tran.charge_slip_number,
        arpb_transactions.original_cvg_id,
        arpb_transactions.original_fc_c as original_fin_class,
        arpb_transactions.proc_id as proc_id,
        arpb_transactions.cpt_code,
        arpb_transactions.modifier_one as modifier_one,
        arpb_transactions.modifier_two as modifier_two,
        arpb_transactions.modifier_three as modifier_three,
        arpb_transactions.modifier_four as modifier_four,
        arpb_transactions.primary_dx_id as dx_one_id,
        arpb_transactions.dx_two_id,
        arpb_transactions.dx_three_id,
        arpb_transactions.dx_four_id,
        arpb_transactions.dx_five_id,
        arpb_transactions.dx_six_id,
        zc_loc_rpt_grp_6.name as location_grouper,
        clarity_loc.loc_name,
        arpb_transactions.loc_id,
        zc_dep_rpt_grp_10.name as division_name,
        clarity_dep.department_name,
        arpb_transactions.department_id,
        clarity_pos.pos_name,
        clarity_pos.pos_code,
        arpb_transactions.pos_id,
        zc_chkin_indicator.name as program_name,
        clarity_tdl_tran.customer_item_one as program_id,
        case
            when
                TO_CHAR(
                    arpb_transactions.service_date, 'mm'
                ) in ('07', '08', '09', '10', '11', '12')
                then TO_CHAR(
                    ADD_MONTHS(arpb_transactions.service_date, 12), 'yyyy'
                )
            else TO_CHAR(arpb_transactions.service_date, 'yyyy')
        end as fiscal_year,
        arpb_transactions.total_match_amt - arpb_transactions.total_mtch_adj as tot_pmt_amt,
        arpb_transactions.total_mtch_ins_amt - arpb_transactions.total_mtch_ins_adj as ins_pmt_amt,
        (arpb_transactions.total_match_amt - arpb_transactions.total_mtch_adj)
        - (
            arpb_transactions.total_mtch_ins_amt - arpb_transactions.total_mtch_ins_adj
        ) as pt_pmt_amt,
        EXTRACT(
            days from CURRENT_DATE - arpb_transactions.service_date
        ) as age_today_dos,
        case
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 30 then '0-30'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 31
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 60 then '31-60'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 61
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 90 then '61-90'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 91
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 120 then '91-120'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 121
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 150 then '121-150'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 151
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 180 then '151-180'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 181
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 210 then '181-210'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 211
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 240 then '211-240'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 241
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 364 then '241-364'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 365
                and EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) <= 720 then '365-720'
            when
                EXTRACT(
                    days from CURRENT_DATE - arpb_transactions.service_date
                ) >= 721 then 'Over 721'
        end as age_category,
        COALESCE(arpb_transactions.payor_id, -2) as cur_payor_id,
        COALESCE(arpb_transactions.original_epm_id, -2) as original_payor_id

    from
        {{ source('clarity_ods', 'clarity_tdl_tran') }} as clarity_tdl_tran
    inner join
        {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions on
            SUBSTR(clarity_tdl_tran.tdl_id, 0, LENGTH(clarity_tdl_tran.tdl_id) - 6) = arpb_transactions.tx_id
    left join
        {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser_bill_prov on
            clarity_ser_bill_prov.prov_id = arpb_transactions.billing_prov_id
    left join {{ source('clarity_ods', 'clarity_ser_2') }} as clarity_ser_2 on
        clarity_ser_2.prov_id = arpb_transactions.billing_prov_id
    left join
        {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser_serv_prov on
            clarity_ser_serv_prov.prov_id = arpb_transactions.serv_provider_id
    left join {{ source('clarity_ods', 'clarity_loc') }} as clarity_loc on
        clarity_loc.loc_id = arpb_transactions.loc_id
    left join
        {{ source('clarity_ods', 'zc_loc_rpt_grp_6') }} as zc_loc_rpt_grp_6 on
            zc_loc_rpt_grp_6.internal_id = clarity_loc.rpt_grp_six
    left join {{ source('clarity_ods', 'clarity_dep') }} as clarity_dep on
        clarity_dep.department_id = arpb_transactions.department_id
    left join
        {{ source('clarity_ods', 'zc_dep_rpt_grp_10') }} as zc_dep_rpt_grp_10 on
            zc_dep_rpt_grp_10.internal_id = clarity_dep.rpt_grp_ten
    left join {{ source('clarity_ods', 'clarity_pos') }} as clarity_pos
        on clarity_pos.pos_id = arpb_transactions.pos_id
    left join
        {{ source('clarity_ods', 'zc_chkin_indicator') }} as zc_chkin_indicator
        on
            zc_chkin_indicator.chkin_indicator_c = clarity_tdl_tran.customer_item_one

    where
        arpb_transactions.tx_type_c = 1
        and clarity_tdl_tran.detail_type = 1
        and arpb_transactions.void_date is null
)

select
    open_ar.tx_id,
    open_ar.tx_num,
    open_ar.post_date,
    open_ar.service_date,
    stg_reimbursement_patient_account.guarantor,
    open_ar.account_id,
    stg_reimbursement_patient_account.patient_name,
    stg_reimbursement_patient_account.birth_date,
    open_ar.patient_id,
    stg_reimbursement_patient_account.mrn,
    stg_reimbursement_payor.member_id,
    open_ar.amount,
    open_ar.pat_outstanding_amt,
    open_ar.ins_outstanding_amt,
    open_ar.total_outstanding_amt,
    open_ar.allowed_amount,
    open_ar.total_match_amt,
    open_ar.total_mtch_ins_adj,
    open_ar.total_mtch_ins_amt,
    open_ar.total_mtch_adj,
    open_ar.procedure_quantity,
    open_ar.zero_balance_yn,
    open_ar.user_id,
    stg_reimbursement_patient_account.pre_cert_num,
    stg_reimbursement_patient_account.auth_num,
    open_ar.billing_prov_name,
    open_ar.billing_prov_npi,
    open_ar.billing_prov_id,
    open_ar.servicing_prov_name,
    open_ar.servicing_prov_id,
    open_ar.cur_cvg_id,
    open_ar.charge_slip_number,
    open_ar.original_cvg_id,
    stg_reimbursement_payor.original_fin_class_name,
    open_ar.original_fin_class,
    stg_reimbursement_proc_dx.proc_name,
    open_ar.proc_id,
    open_ar.cpt_code,
    stg_reimbursement_proc_dx.cpt_cat,
    open_ar.modifier_one,
    open_ar.modifier_two,
    open_ar.modifier_three,
    open_ar.modifier_four,
    stg_reimbursement_proc_dx.dx_one_name,
    stg_reimbursement_proc_dx.dx_one_icd,
    open_ar.dx_one_id,
    stg_reimbursement_proc_dx.dx_two_name,
    stg_reimbursement_proc_dx.dx_two_icd,
    open_ar.dx_two_id,
    stg_reimbursement_proc_dx.dx_three_name,
    stg_reimbursement_proc_dx.dx_three_icd,
    open_ar.dx_three_id,
    stg_reimbursement_proc_dx.dx_four_name,
    stg_reimbursement_proc_dx.dx_four_icd,
    open_ar.dx_four_id,
    stg_reimbursement_proc_dx.dx_five_name,
    stg_reimbursement_proc_dx.dx_five_icd,
    open_ar.dx_five_id,
    stg_reimbursement_proc_dx.dx_six_name,
    stg_reimbursement_proc_dx.dx_six_icd,
    open_ar.dx_six_id,
    open_ar.location_grouper,
    open_ar.loc_name,
    open_ar.loc_id,
    open_ar.division_name,
    open_ar.department_name,
    open_ar.department_id,
    open_ar.pos_name,
    open_ar.pos_code,
    open_ar.pos_id,
    open_ar.program_name,
    open_ar.program_id,
    stg_reimbursement_invoice.max_invoice_number,
    stg_reimbursement_invoice.eob_icn,
    open_ar.fiscal_year,
    open_ar.tot_pmt_amt,
    open_ar.ins_pmt_amt,
    open_ar.pt_pmt_amt,
    open_ar.age_today_dos,
    open_ar.age_category,
    stg_reimbursement_payor.cur_payor_name,
    open_ar.cur_payor_id,
    stg_reimbursement_payor.cur_fin_class,
    stg_reimbursement_payor.cur_fin_class_id,
    stg_reimbursement_payor.cur_benefit_plan_name,
    stg_reimbursement_payor.cur_benefit_plan_id,
    stg_reimbursement_payor.original_plan_name,
    stg_reimbursement_payor.original_plan_id,
    stg_reimbursement_payor.original_payor_name,
    open_ar.original_payor_id,
    stg_reimbursement_denials.first_rej_post_dt_key,
    stg_reimbursement_denials.first_rej_cat_seq_1,
    stg_reimbursement_denials.first_rej_cd_seq_1,
    stg_reimbursement_denials.first_rej_desc_seq_1,
    stg_reimbursement_denials.last_rej_post_dt_key,
    stg_reimbursement_denials.last_rej_cat_seq_1,
    stg_reimbursement_denials.last_rej_cd_seq_1,
    stg_reimbursement_denials.last_rej_desc_seq_1,
    stg_reimbursement_denials.last_rej_cat_seq_2,
    stg_reimbursement_denials.last_rej_cd_seq_2,
    stg_reimbursement_denials.last_rej_desc_seq_2,
    stg_reimbursement_denials.last_rej_cat_seq_3,
    stg_reimbursement_denials.last_rej_cd_seq_3,
    stg_reimbursement_denials.last_rej_desc_seq_3
from open_ar
left join {{ ref('stg_reimbursement_denials') }} as stg_reimbursement_denials on
    open_ar.tx_id = stg_reimbursement_denials.tx_id and open_ar.cur_cvg_id = stg_reimbursement_denials.cvg_id
left join {{ ref('stg_reimbursement_invoice') }} as stg_reimbursement_invoice on
    open_ar.tx_id = stg_reimbursement_invoice.tx_id
left join {{ ref('stg_reimbursement_patient_account') }} as stg_reimbursement_patient_account on
    open_ar.tx_id = stg_reimbursement_patient_account.tx_id
left join {{ ref('stg_reimbursement_proc_dx') }} as stg_reimbursement_proc_dx on
    open_ar.tx_id = stg_reimbursement_proc_dx.tx_id
left join {{ ref('stg_reimbursement_payor') }} as stg_reimbursement_payor on
    open_ar.tx_id = stg_reimbursement_payor.tx_id
