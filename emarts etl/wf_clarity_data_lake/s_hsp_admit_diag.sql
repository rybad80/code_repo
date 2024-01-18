/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_hsp_admit_diag",
  "MAPPING_NAME": "m_stg_load_cl_hsp_admit_diag",
  "MAPPING_ID": 7918,
  "TARGET_ID": 7583,
  "TARGET_NAME": "s_hsp_admit_diag"
}
*/

with
sq_hsp_admit_diag as (
    select
        admit_diag_text,
        cm_ct_owner_id,
        dx_id,
        line,
        pat_enc_csn_id,
        pat_enc_date_real,
        pat_id
    from {{ source('clarity_ods', 'hsp_admit_diag') }}

)
select
    cast(sq_hsp_admit_diag.pat_id as varchar(18)) as pat_id,
    cast(sq_hsp_admit_diag.pat_enc_date_real as real) as pat_enc_date_real,
    cast(sq_hsp_admit_diag.line as bigint) as line,
    cast(sq_hsp_admit_diag.dx_id as bigint) as dx_id,
    cast(sq_hsp_admit_diag.admit_diag_text as varchar(255)) as admit_diag_text,
    cast(sq_hsp_admit_diag.cm_ct_owner_id as varchar(25)) as cm_ct_owner_id,
    cast(sq_hsp_admit_diag.pat_enc_csn_id as bigint) as pat_enc_csn_id
from sq_hsp_admit_diag
