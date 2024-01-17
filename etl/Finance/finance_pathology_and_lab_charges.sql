{{ config(meta = {
    'critical': true
}) }}

select
    cost_center.cost_cntr_id as cost_cntr_id,
    cost_center.gl_comp as cost_center_code,
    cost_center.cost_cntr_nm as cost_center_name,
    cost_center.rpt_grp_1 as cost_center_site,
    post_date.fy_yyyy as fiscal_year,
    post_date.c_yyyy as calendar_year,
    post_date.c_mm as calendar_month,
    post_date.month_nm as month_name,
    procedure.proc_cd as procedure_code,
    procedure.proc_nm as procedure_name,
    fact_transaction_hb.hb_proc_desc as procedure_hb_description,
    rsch_ind.dict_nm as hospital_research_indicator,
    coalesce(research_study.res_stdy_nm, 'N/A') as hospital_research_study_name,
    case
        when
            dict_acct_class.src_id in (1, 9, 12) then 'Inpatient'
        else 'Outpatient'
    end as transaction_ip_op_class,
    sum(fact_transaction_hb.chrg_amt) as charge_amount,
    sum(fact_transaction_hb.chg_qty) as charge_quantity
from {{source('cdw', 'fact_transaction_hb')}} as fact_transaction_hb
left join {{source('cdw', 'master_date')}} as post_date
    on fact_transaction_hb.post_dt_key = post_date.dt_key
left join {{source('cdw', 'cost_center')}} as cost_center
    on fact_transaction_hb.cost_cntr_key = cost_center.cost_cntr_key
left join {{source('cdw', 'cdw_dictionary')}} as dict_acct_class
    on fact_transaction_hb.dict_acct_class_key = dict_acct_class.dict_key
left join {{source('cdw', 'cdw_dictionary')}} as dict_trans_type
    on fact_transaction_hb.trans_type_key = dict_trans_type.dict_key
left join {{source('cdw', 'procedure')}} as procedure
    on fact_transaction_hb.proc_key = procedure.proc_key
left join {{source('cdw', 'hospital_account')}} as hospital_account
    on fact_transaction_hb.hsp_acct_key = hospital_account.hsp_acct_key
left join {{source('cdw', 'research_study')}} as research_study
    on hospital_account.res_stdy_key = research_study.res_stdy_key
left join {{source('cdw', 'cdw_dictionary')}} as rsch_ind
    on hospital_account.rsrch_ind = rsch_ind.dict_key
inner join {{ref('dim_cost_center_hierarchy')}} as dim_cost_center_hierarchy
    on cost_center.gl_comp = dim_cost_center_hierarchy.cost_cntr_id
where
    post_date.dt_key >= 20180701
    and dict_trans_type.dict_nm = 'Charge'
    and dim_cost_center_hierarchy.cost_center_hierarchy_level4_name = 'Pathology and Lab Medicine'
group by
    cost_center.cost_cntr_id,
    cost_center.gl_comp,
    cost_center.cost_cntr_nm,
    cost_center.rpt_grp_1,
    post_date.fy_yyyy,
    post_date.c_yyyy,
    post_date.c_mm,
    post_date.month_nm,
    procedure.proc_cd,
    procedure.proc_nm,
    fact_transaction_hb.hb_proc_desc,
    transaction_ip_op_class,
    rsch_ind.dict_nm,
    hospital_research_study_name
