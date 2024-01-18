select
    prov1.prov_key as "Attending Provider Key",
    prov1.prov_id as "Attending Provider ID",
    prov1.full_nm as "Attending Provider Name",
    loc1.loc_id as "Admission Location ID",
    loc1.loc_key as "Admission Location Key",
    loc1.loc_nm as "Admission Location Name",
    prov2.prov_key as "Admission Provider Key",
    prov2.prov_id as "Admission Provider ID",
    prov2.full_nm as "Admission Provider Name",
    dept1.dept_key as "Admission Department Key",
    dept1.dept_id as "Admission Department ID",
    dept1.dept_nm as "Admission Department Name",
    dept1.dept_abbr as "Admission Department Abbr",
    dept1.specialty as "Admission Department Specialty",
    loc2.loc_id as "Discharge Location ID",
    loc2.loc_key as "Discharge Location Key",
    loc2.loc_nm as "Discharge Location Name",
    dict8.dict_nm as "Hospital Admission Source",
    ha.hsp_acct_key as "Hospital Account Key",
    ha.hsp_acct_id as "Hospital Account ID",
    ha.hsp_acct_nm as "Hospital Account Name",
    ha.base_inv_num as "Hospital Account Base Inv Num",
    ha.acct_bill_dt as "Hospital Account Billed Date",
    ha.acct_close_dt as "Hospital Account Closed Date",
    ha.claim_id as "Hospital Account Claim ID",
    dict2.dict_nm as "Hospital Account Class",
    bk1.bkt_id as "Hospital Bad DebtBucket ID",
    col1.coll_agncy_nm as "Hospital Bad Debt Coll Agency",
    dict1.dict_nm as "Hospital Base Class",
    ha.er_adm_dt as "Hospital Billing ED Adm Date",
    ha.er_disch_dt as "Hospital Billing ED Disch Date",
    ha.bill_note_exp_dt as "Hospital Bill Note Expire Date",
    dict3.dict_nm as "Hospital Billing Patient Class",
    pyr.payor_id as "Hospital Billing Payor ID",
    pyr.payor_key as "Hospital Billing Payor Key",
    pyr.payor_nm as "Hospital Billing Payor Name",
    bp.bp_key as "Hospital Billing key",
    bp.bp_id as "Hospital Billing Plan ID",
    bp.bp_nm as "Hospital Billing Plan Name",
    bp.prod_type as "Hospital Billing Prod Type",
    ha.comb_hsp_acct_id as "Hospital Combine Account ID",
    dict9.dict_nm as "Hospital Discharge Dest Name",
    ha.disch_to as "Hospital Discharge To",
    ha.drg_exp_reimburse_amt as "Hospital DRG Expect Reimb Amt",
    dict10.dict_nm as "Hospital DRG Type",
    ha.bill_drg_mdc_val as "Hospital DRG MDC Value",
    ha.bill_drg_weight as "Hospital DRG Weight",
    ha.bill_drg_ps as "Hospital DRG PS",
    ha.bill_drg_rom as "Hospital DRG ROM",
    ha.bill_drg_short_los as "Hospital DRG Short LOS",
    ha.bill_drg_long_los as "Hospital DRG Long LOS",
    ha.bill_drg_amlos as "Hopsital DRG AMLOS",
    ha.bill_drg_gmlos as "Hospital DRG GMLOS",
    ha.cvg_key as "Hospital Primary Coverage Key",
    cvg.cvg_id as "Hospital Primary Coverage ID",
    dept2.dept_key as "Discharge Department Key",
    dept2.dept_id as "Discharge Department ID",
    dept2.dept_nm as "Discharge Department Name",
    dept2.dept_abbr as "Discharge Department Abbr",
    dept2.specialty as "Discharge Department Specialty",
    dict4.dict_nm as "Hospital Primary Service",
    dict5.dict_nm as "Hospital Admission Type",
    dict6.dict_nm as "Hospital Billing Status",
    dict7.dict_nm as "Hospital Billing Coding Status",
    drg.drg_num as "Hospital Final DRG Number",
    drg.drg_nm as "Hospital Final DRG Name",
    fc.fc_key as "Hospital Financial Key",
    fc.fc_nm as "Hospital Financial Class",
    fc.fc_abbr as "Hospital Fin Class Abbr",
    fc.fc_title as "Hospital Fin Class Title",
    ha.first_det_bill_dt as "Hospital First Det Bill Date",
    ha.first_dmnd_stmt_dt as "Hospital First Dem Stmt Date",
    ha.first_stmt_dt as "Hospital First Stmt Date",
    ha.dnb_dt as "Discharged Not Billed Date",
    ha.tot_chrgs as "Hospital Account Charges",
    ha.tot_pmts as "Hospital Account Payments",
    ha.tot_adj as "Hospital Account Adjustments",
    ha.tot_acct_bal as "Hospital Account Balance",
    ha.acct_zero_bal_dt as "Hosptial Account Zero Bal Date",
    loc3.loc_id as "Hospital Account Location ID",
    loc3.loc_nm as "Hospital Account Location Name",
    ga.acct_id as "Hospital Guarantor ID",
    ga.acct_nm as "Hospital Guarantor Name",
    ga.dob as "Hospital Guarantor DOB",
    ga.sex as "Hospital Guarantor Gender",
    ga.ssn as "Hospital Guarantor SSN",
    ga.bill_addr_line1 as "Hospital Guarantor Addr Line1",
    ga.bill_addr_line2 as "Hospital Guarantor Addr Line2",
    ga.city as "Hospital Guarantor City",
    dict15.dict_nm as "Hospital Guarantor County",
    dict16.dict_nm as "Hospital Guarantor Country",
    ga."STATE" as "Hospital Guarantor State",
    ga.zip as "Hospital Guarantor Zip Code",
    ga.home_ph as "Hospital Guarantor Home Phone",
    ga.work_ph as "Hospital Guarantor Work Phone",
    date(ha.adm_dt) as "Hospital Acct Adm Date",
    to_char(ha.adm_dt, 'HH24:MI:SS' :: "VARCHAR") as "Hospital Acct Adm Time",
    date(ha.disch_dt) as "Hospital Acct Disch Date",
    to_char(ha.disch_dt, 'HH24:MI:SS' :: "VARCHAR") as "Hospital Acct Disch Time",
    ha.hb_pat_nm as "Hospital Billing Pat Name",
    ha.hb_pat_mrn as "Hospital Billing Pat MRN",
    pat.pat_key as "Hospital Billing Pat Key",
    pat.pat_id as "Hospital Billing Pat ID",
    pat.sex as "Hospital Billing Pat Gender",
    pat.dob as "Hospital Billing Pat DOB",
    ha.hb_pat_addr_line1 as "Hospital Billing Pat Addr Ln1",
    ha.hb_pat_addr_line2 as "Hospital Billing Pat Addr Ln2",
    ha.hb_pat_city as "Hospital Billing Pat City",
    ha.hb_pat_state as "Hospital Billing Pat State",
    ha.hb_pat_zip as "Hospital Billing Pat Zip",
    ha.hb_pat_county as "Hospital Billing Pat County",
    ha.hb_pat_country as "Hospital Billing Pat Country",
    ha.hb_pat_phone as "Hospital Billing Pat Phone",
    rsh.res_stdy_id as "Hospital Research Study ID",
    rsh.res_stdy_nm as "Hospital Research Study Name",
    rsh.stdy_cd as "Hospital Research Study Code",
    dict11.dict_nm as "Hospital Research Indicator",
    prov3.prov_key as "Hospital Refer Provider Key",
    prov3.prov_id as "Hospital Refer Provider ID",
    prov3.full_nm as "Hospital Refer Provider Name",
    sva.svc_area_id as "Hospital Service Area ID",
    sva.svc_area_nm as "Hospital Service Area Name",
    bk2.bkt_id as "Hosp Selfpay Liab Bucket ID",
    ha.treatment_auth_num as "Hosp Treatment Auth Num",
    ha.ub92_coins_days as "UB92 Coinsurance Days",
    ha.ub92_covered_days as "UB92 Covered Days",
    ha.ub92_lifetime_days as "UB92 Lifetime Days",
    ha.ub92_noncovrd_days as "UB92 Non-covered Days",
    ha.ub92_tob_override as "UB92 TOB Override",
    bk3.bkt_id as "Undistributed Liab Bucket ID",
    dict12.dict_nm as "Hosp Institute Indicator",
    dict13.dict_nm as "Hosp Payment Plan Indicator",
    ha.last_det_bill_dt as "Last Detail Bill Date",
    ha.last_dmnd_stmt_dt as "Last Demand Statement Date",
    ha.last_intrm_bill_dt as "Last Interim Bill Date",
    ha.last_stmt_dt as "Last Statement Date",
    dict14.dict_nm as "Means Of Arrival",
    ha.next_stmt_dt as "Next Statement Date",
    ha.num_of_det_bills as "Number Of Detail Bills",
    ha.num_of_dmnd_stmts as "Number Of Demand Statements",
    ha.num_of_stmts_sent as "Number Of Statements Sent",
    bk4.bkt_id as "Prebill Liab Bucket Id",
    ha.prorated_pat_bal as "Pro-Rated Patient Balance",
    ha.prorated_pat_liab as "Pro-Rated Patient Liability",
    vis.enc_id as "Primary Encounter Id",
    ha.adm_dt_key as "Hospital Admit Date Key",
    ha.hb_guar_nm as "Reg Guarantor Name",
    ha.hb_guar_ssn as "Reg Guarantor SSN",
    dict19.dict_nm as "Reg Guarantor Gender",
    ha.hb_guar_dob as "Reg Guarantor Birth Date",
    ha.hb_guar_addr_line1 as "Reg Guarantor Addr1",
    ha.hb_guar_addr_line2 as "Reg Guarantor Addr2",
    ha.hb_guar_city as "Reg Guarantor City",
    dict20.dict_nm as "Reg Guarantor State",
    ha.hb_guar_zip as "Reg Guarantor Zip",
    dict17.dict_nm as "Reg Guarantor Country",
    dict18.dict_nm as "Reg Guarantor County",
    ha.hb_guar_home_ph as "Reg Guarantor Home Phone",
    ha.hb_guar_work_ph as "Reg Guarantor Work Phone",
    ga.acct_key as "Hospital Guarantor Key",
    case
        when (
            (ha.bad_debt_flag_ind = '1' :: int1)
            and (
                (ha.extern_ar_flag_ind = '0' :: int1)
                or (ha.extern_ar_flag_ind = '-2' :: int1)
            )
        ) then 'BAD DEBT' :: "VARCHAR"
        when (
            (ha.extern_ar_flag_ind = '1' :: int1)
            and (
                (ha.bad_debt_flag_ind = '0' :: int1)
                or (ha.bad_debt_flag_ind = '-2' :: int1)
            )
        ) then 'EXTERNAL AR' :: "VARCHAR"
        else 'NONE' :: "VARCHAR"
    end as "AR Collection Status"
from
    {{ source('cdw', 'hospital_account') }} ha
    left join {{ source('cdw', 'payor') }} pyr on ((ha.pri_payor_key = pyr.payor_key))
    left join {{ source('cdw', 'financial_class') }} fc on ((fc.fc_key = ha.fc_key))
    left join {{ source('cdw', 'department') }} dept1 on ((ha.adm_dept_key = dept1.dept_key))
    left join {{ source('cdw', 'department') }} dept2 on ((ha.disch_dept_key = dept2.dept_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict1 on ((ha.dict_acct_basecls_key = dict1.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict2 on ((ha.dict_acct_class_key = dict2.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict3 on ((ha.dict_pat_stat_key = dict3.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict4 on ((ha.dict_pri_svc_key = dict4.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict5 on ((ha.dict_adm_type_key = dict5.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict6 on ((ha.dict_bill_stat_key = dict6.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict7 on ((ha.dict_coding_stat_key = dict7.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict8 on ((ha.dict_adm_src_key = dict8.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict9 on ((ha.dict_disch_dest_key = dict9.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict10 on ((ha.dict_bill_drgtype_key = dict10.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict11 on ((ha.rsrch_ind = dict11.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict12 on ((ha.dict_insti_key = dict12.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict13 on ((ha.dict_pmtplan_amt_due_key = dict13.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict14 on ((ha.dict_means_of_arrvl_key = dict14.dict_key))
    left join {{ source('cdw', 'account') }} ga on ((ha.acct_key = ga.acct_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict15 on ((ga.dict_county_key = dict15.dict_key))
    left join {{ source('cdw', 'provider') }} prov1 on ((prov1.prov_key = ha.attend_prov_key))
    left join {{ source('cdw', 'provider') }} prov2 on ((prov2.prov_key = ha.adm_prov_key))
    left join {{ source('cdw', 'provider') }} prov3 on ((prov3.prov_key = ha.ref_prov_key))
    left join {{ source('cdw', 'location') }} loc1 on ((loc1.loc_key = ha.adm_loc_key))
    left join {{ source('cdw', 'location') }} loc2 on ((loc2.loc_key = ha.disch_loc_key))
    left join {{ source('cdw', 'location') }} loc3 on ((loc3.loc_key = ha.loc_key))
    left join {{ source('cdw', 'collection_agency') }} col1 on ((col1.coll_agncy_key = ha.bad_debt_coll_agncy_key))
    left join {{ source('cdw', 'liability_bucket') }} bk1 on ((bk1.liab_bkt_key = ha.bad_debt_liab_bkt_key))
    left join {{ source('cdw', 'liability_bucket') }} bk2 on ((bk2.liab_bkt_key = ha.self_pay_liab_bkt_key))
    left join {{ source('cdw', 'liability_bucket') }} bk3 on ((bk3.liab_bkt_key = ha.undist_liab_bkt_key))
    left join {{ source('cdw', 'liability_bucket') }} bk4 on ((bk4.liab_bkt_key = ha.prebill_liab_bkt_key))
    left join {{ source('cdw', 'diagnosis_group') }} drg on ((drg.drg_key = ha.drg_key))
    left join {{ source('cdw', 'coverage') }} cvg on ((ha.cvg_key = cvg.cvg_key))
    left join {{ source('cdw', 'benefit_plan') }} bp on ((bp.bp_key = ha.pri_bp_key))
    left join {{ source('cdw', 'research_study') }} rsh on ((rsh.res_stdy_key = ha.res_stdy_key))
    left join {{ source('cdw', 'service_area') }} sva on ((sva.svc_area_key = ha.svc_area_key))
    left join {{ source('cdw', 'visit') }} vis on ((vis.visit_key = ha.pri_visit_key))
    left join {{ source('cdw', 'patient') }} pat on ((pat.pat_key = ha.pat_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict16 on ((ga.dict_country_key = dict16.dict_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict17 on ((dict17.dict_key = ha.dict_guar_country_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict18 on ((dict18.dict_key = ha.dict_guar_county_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict19 on ((dict19.dict_key = ha.dict_guar_sex_key))
    left join {{ source('cdw', 'cdw_dictionary') }} dict20 on ((dict20.dict_key = ha.dict_guar_state_key))