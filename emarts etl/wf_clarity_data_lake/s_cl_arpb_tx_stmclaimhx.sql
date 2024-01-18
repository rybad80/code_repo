/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_arpb_tx_stmclaimhx",
  "MAPPING_NAME": "m_stg_load_cl_arpb_tx_stmclaimhx",
  "MAPPING_ID": 7914,
  "TARGET_ID": 7632,
  "TARGET_NAME": "s_cl_arpb_tx_stmclaimhx"
}
*/


with sq_arpb_tx_stmclaimhx as (
    select
        arpb_tx_stmclaimhx.tx_id,
        arpb_tx_stmclaimhx.line,
        arpb_tx_stmclaimhx.cm_phy_owner_id,
        arpb_tx_stmclaimhx.cm_log_owner_id,
        arpb_tx_stmclaimhx.bc_hx_type_c,
        arpb_tx_stmclaimhx.bc_hx_date,
        arpb_tx_stmclaimhx.bc_hx_coverage_id,
        arpb_tx_stmclaimhx.bc_hx_assigned_yn,
        arpb_tx_stmclaimhx.bc_hx_amount,
        arpb_tx_stmclaimhx.bc_hx_invoice_num,
        arpb_tx_stmclaimhx.bc_hx_payment_amt,
        arpb_tx_stmclaimhx.bc_hx_payment_date,
        arpb_tx_stmclaimhx.bc_hx_payor_id,
        arpb_tx_stmclaimhx.bc_hx_resubmit_date,
        arpb_tx_stmclaimhx.bc_hx_clm_db_id,
        arpb_tx_stmclaimhx.bc_hx_held_amount,
        arpb_tx_stmclaimhx.bc_hx_form_id,
        arpb_tx_stmclaimhx.bc_hx_bo_proc_id,
        arpb_tx_stmclaimhx.bc_hx_aux_proc,
        arpb_tx_stmclaimhx.bc_hx_accept_date,
        arpb_tx_stmclaimhx.bc_hx_first_clm_flg,
        arpb_tx_stmclaimhx.bc_hx_ar_class_c,
        arpb_tx_stmclaimhx.bc_hx_fdf_id,
        x_cl_etr_tx_id.update_date
    from
         {{ source('clarity_ods', 'arpb_tx_stmclaimhx') }} as arpb_tx_stmclaimhx
        inner join {{ source('clarity_ods', 'x_cl_etr_tx_id') }} as x_cl_etr_tx_id
            on x_cl_etr_tx_id.tx_id = arpb_tx_stmclaimhx.tx_id
)
    select
        cast(sq_arpb_tx_stmclaimhx.tx_id as bigint) as tx_id,
        cast(sq_arpb_tx_stmclaimhx.line as bigint) as line,
        cast(sq_arpb_tx_stmclaimhx.cm_phy_owner_id as varchar(25)) as cm_phy_owner_id,
        cast(sq_arpb_tx_stmclaimhx.cm_log_owner_id as varchar(25)) as cm_log_owner_id,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_type_c as bigint) as bc_hx_type_c,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_date as timestamp) as bc_hx_date,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_coverage_id as bigint) as bc_hx_coverage_id,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_assigned_yn as char(1)) as bc_hx_assigned_yn,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_amount as numeric) as bc_hx_amount,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_invoice_num as varchar(254)) as bc_hx_invoice_num,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_payment_amt as numeric) as bc_hx_payment_amt,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_payment_date as timestamp) as bc_hx_payment_date,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_payor_id as bigint) as bc_hx_payor_id,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_resubmit_date as timestamp) as bc_hx_resubmit_date,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_clm_db_id as bigint) as bc_hx_clm_db_id,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_held_amount as varchar(254)) as bc_hx_held_amount,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_form_id as varchar(18)) as bc_hx_form_id,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_bo_proc_id as bigint) as bc_hx_bo_proc_id,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_aux_proc as varchar(254)) as bc_hx_aux_proc,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_accept_date as timestamp) as bc_hx_accept_date,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_first_clm_flg as varchar(254)) as bc_hx_first_clm_flg,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_ar_class_c as bigint) as bc_hx_ar_class_c,
        cast(sq_arpb_tx_stmclaimhx.bc_hx_fdf_id as varchar(18)) as bc_hx_fdf_id
    from
        sq_arpb_tx_stmclaimhx
