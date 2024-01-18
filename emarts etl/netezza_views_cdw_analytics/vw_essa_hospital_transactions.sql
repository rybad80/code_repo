select
    dict1.dict_nm as "Account Class Name",
    htr.allow_amt as "Allowed Amount",
    htr.bd_adj_amt as "Bad Debt Adjusted Amount",
    htr.bp_key as "Benefit Plan Key",
    htr.billed_amt as "Billed Amount",
    prov.prov_key as "Billing Provider Key",
    prov.prov_id as "Billing Provider ID",
    prov.full_nm as "Billing Provider Name",
    htr.chrg_amt as "Charge Amount",
    htr.chg_qty as "Charge Quantity",
    coll.coll_agncy_id as "Collection Agency ID",
    coll.coll_agncy_key as "Collection Agency Key",
    coll.coll_agncy_nm as "Collection Agency Name",
    htr.coins_amt as "Coinsurance Amount",
    htr.con_adj_amt as "Contractual Adjusted Amount",
    htr.copay_amt as "Copay Amount",
    htr.copay_ind as "Copay Indicator",
    ccntr.cost_cntr_key as "Cost Center Key",
    ccntr.cost_cntr_id as "Cost Center ID",
    ccntr.cost_cntr_nm as "Cost Center Name",
    htr.cpt_cd as "CPT Code",
    htr.ded_amt as "Deductible Amount",
    dept.dept_id as "Department ID",
    dept.dept_key as "Department Key",
    dept.dept_nm as "Department Name",
    finc.fc_id as "Financial Class ID",
    finc.fc_key as "Financial Class Key",
    finc.fc_nm as "Financial Class Name",
    htr.credit_gl_num as "GL Credit Number",
    htr.debit_gl_num as "GL Debit Number",
    hsp.hsp_acct_id as "Hospital Account ID",
    hsp.hsp_acct_key as "Hospital Account Key",
    htr.hcpcs_cd as "HCPCS Code",
    htr.inv_num as "Invoice Number",
    liab.bkt_id as "Liability Bucket ID",
    htr.misc_bs_adj_amt as "Misc BS Adjusted Amount",
    htr.misc_pl_adj_amt as "Misc PL Adjusted Amount",
    mod1.mod_key as "Modifier Key",
    mod1.src_ext_id as "Modifier ID",
    mod1.mod_nm as "Modifier Name",
    htr.non_cv_amt as "Non Covered Amount",
    htr.org_price as "Orginal Price",
    htr.other_adj_amt as "Other Adjustment Amount",
    vis.enc_id as "Visit ID",
    vis.visit_key as "Visit Key",
    htr.pmt_amt as "Payment Amount",
    dict2.dict_key as "Payment Source Key",
    dict2.src_id as "Payment Source ID",
    dict2.dict_nm as "Payment Source Name",
    payr.payor_key as "Payor Key",
    payr.payor_id as "Payor ID",
    payr.payor_nm as "Payor Name",
    prov2.prov_key as "Perf Provider Key",
    prov2.prov_id as "Perf Provider ID",
    prov2.full_nm as "Perf Provider Name",
    htr.post_batch_num as "Post Batch Number",
    fee.fee_sched_id as "Fee Schedule ID",
    fee.fee_sched_key as "Fee Schedule Key",
    fee.fee_sched_nm as "Fee Schedule Name",
    mdt2.full_dt as "Post Date",
    proc.proc_id as "Procedure ID",
    proc.proc_key as "Procedure Key",
    proc.proc_nm as "Procedure Name",
    htr.hb_proc_desc as "Procedure HB Description",
    htr.ref_num as "Referral Number",
    loc.loc_key as "Revenue Location Key",
    loc.loc_id as "Revenue Location ID",
    loc.loc_nm as "Revenue Location Name",
    htr.rvu,
    sva.svc_area_id as "Service Area ID",
    sva.svc_area_key as "Service Area Key",
    sva.svc_area_nm as "Service Area Name",
    styp.svc_nm as "Service Type Name",
    mdt.full_dt as "Service Date",
    htr.tx_id as "Transaction ID",
    htr.trans_cmt as "Transaction Comment",
    dict3.dict_nm as "Transaction Type",
    htr.dict_acct_class_key as "Account Class Key",
    case
        when (
            (dict4.src_id = ('1' :: numeric(1, 0)) :: numeric(1, 0))
            or (dict4.src_id = ('9' :: numeric(1, 0)) :: numeric(1, 0))
        ) then 'Inpatient' :: "VARCHAR"
        else 'Outpatient' :: "VARCHAR"
    end as "Transaction IP-OP Class",
    dict4.dict_nm as "Transaction Account Class",
    htr.txn_num as "Transaction Number",
    rev.rev_cd_key as "UB Revenue Key",
    rev.rev_cd as "UB Revenue Code",
    rev.rev_cd_nm as "UB Revenue Code Name",
    emp.full_nm as "User Name",
    htr.hb_pat_nm as "Inst Bill Patient Name",
    htr.pat_key as "Patient Key",
    dict5.dict_nm as "Transaction Source",
    htr.med_key as "Medication Key",
    htr.ndc_key as "NDC Key"
from
    {{source('cdw', 'fact_transaction_hb')}} htr
    left join {{source('cdw', 'cdw_dictionary')}} dict1 on ((dict1.dict_key = htr.dict_acct_class_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict2 on ((dict2.dict_key = htr.dict_pmt_src_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict3 on ((dict3.dict_key = htr.trans_type_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict4 on ((dict4.dict_key = htr.dict_acct_class_key))
    left join {{source('cdw', 'provider')}} prov on ((prov.prov_key = htr.bill_prov_key))
    left join {{source('cdw', 'liability_bucket')}} liab on ((liab.liab_bkt_key = htr.liab_bkt_key))
    left join {{source('cdw', 'collection_agency')}} coll on ((coll.coll_agncy_key = htr.coll_agncy_key))
    left join {{source('cdw', 'cost_center')}} ccntr on ((ccntr.cost_cntr_key = htr.cost_cntr_key))
    left join {{source('cdw', 'department')}} dept on ((dept.dept_key = htr.dept_key))
    left join {{source('cdw', 'financial_class')}} finc on ((finc.fc_key = htr.fc_key))
    left join {{source('cdw', 'hospital_account')}} hsp on ((hsp.hsp_acct_key = htr.hsp_acct_key))
    left join {{source('cdw', 'master_modifier')}} mod1 on ((mod1.mod_key = htr.mod1_key))
    left join {{source('cdw', 'visit')}} vis on ((vis.visit_key = htr.visit_key))
    left join {{source('cdw', 'payor')}} payr on ((payr.payor_key = htr.payor_key))
    left join {{source('cdw', 'provider')}} prov2 on ((prov2.prov_key = htr.svc_prov_key))
    left join {{source('cdw', 'fee_schedule')}} fee on ((fee.fee_sched_key = htr.fee_sched_key))
    left join {{source('cdw', 'procedure')}} proc on ((proc.proc_key = htr.proc_key))
    left join {{source('cdw', 'location')}} loc on ((loc.loc_key = htr.trans_loc_key))
    left join {{source('cdw', 'service_area')}} sva on ((sva.svc_area_key = htr.svc_area_key))
    left join {{source('cdw', 'master_date')}} mdt on ((mdt.dt_key = htr.svc_dt_key))
    left join {{source('cdw', 'master_date')}} mdt2 on ((mdt2.dt_key = htr.post_dt_key))
    left join {{source('cdw', 'revenue_code')}} rev on ((rev.rev_cd_key = htr.rev_cd_key))
    left join {{source('cdw', 'employee')}} emp on ((emp.emp_key = htr.user_emp_key))
    left join {{source('cdw', 'master_service_type')}} styp on ((htr.svc_type_key = styp.svc_type_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict5 on ((htr.dict_tx_src_key = dict5.dict_key))