/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_hsp_acct_admit_dx",
  "MAPPING_NAME": "m_stg_load_cl_hsp_acct_admit_dx",
  "MAPPING_ID": 7917,
  "TARGET_ID": 7599,
  "TARGET_NAME": "s_hsp_acct_admit_dx"
}
*/

with
sq_hsp_acct_admit_dx as (
    select
        admit_dx_id,
        admit_dx_text,
        cm_log_owner_id,
        cm_phy_owner_id,
        hsp_account_id,
        line
    from {{ source('clarity_ods', 'hsp_acct_admit_dx') }}

)
select
    cast(sq_hsp_acct_admit_dx.hsp_account_id as bigint) as hsp_account_id,
    cast(sq_hsp_acct_admit_dx.line as bigint) as line,
    cast(sq_hsp_acct_admit_dx.admit_dx_id as bigint) as admit_dx_id,
    cast(
        sq_hsp_acct_admit_dx.admit_dx_text as varchar(255)
    ) as admit_dx_text
from sq_hsp_acct_admit_dx
