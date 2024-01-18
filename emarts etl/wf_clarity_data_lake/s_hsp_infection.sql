/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_hsp_infection",
  "MAPPING_NAME": "m_stg_load_cl_hsp_infection",
  "MAPPING_ID": 7928,
  "TARGET_ID": 7645,
  "TARGET_NAME": "s_hsp_infection"
}
*/

with
sq_hsp_infection as (
    select
        cm_ct_owner_id,
        infection_c,
        inf_add_el_time,
        inf_add_el_user_id,
        inf_cmt_el,
        inf_rsv_el_time,
        inf_rsv_el_user_id,
        line,
        pat_enc_csn_id,
        pat_enc_date_real,
        pat_id
    from {{ source('clarity_ods', 'hsp_infection') }}
)
select
    cast(sq_hsp_infection.pat_id as varchar(18)) as pat_id,
    cast(
        sq_hsp_infection.pat_enc_date_real as numeric
    ) as pat_enc_date_real,
    cast(sq_hsp_infection.line as bigint) as line,
    cast(sq_hsp_infection.infection_c as bigint) as infection_c,
    cast(sq_hsp_infection.inf_add_el_time as timestamp) as inf_add_el_time,
    cast(
        sq_hsp_infection.inf_add_el_user_id as varchar(18)
    ) as inf_add_el_user_id,
    cast(sq_hsp_infection.inf_rsv_el_time as timestamp) as inf_rsv_el_time,
    cast(
        sq_hsp_infection.inf_rsv_el_user_id as varchar(18)
    ) as inf_rsv_el_user_id,
    cast(sq_hsp_infection.cm_ct_owner_id as varchar(25)) as cm_ct_owner_id,
    cast(sq_hsp_infection.pat_enc_csn_id as bigint) as pat_enc_csn_id,
    cast(sq_hsp_infection.inf_cmt_el as varchar(254)) as inf_cmt_el
from sq_hsp_infection
