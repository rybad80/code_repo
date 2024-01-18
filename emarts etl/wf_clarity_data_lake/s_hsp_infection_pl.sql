/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_hsp_infection_pl",
  "MAPPING_NAME": "m_stg_load_cl_hsp_infection_pl",
  "MAPPING_ID": 7924,
  "TARGET_ID": 7643,
  "TARGET_NAME": "s_hsp_infection_pl"
}
*/

with
sq_hsp_infection_pl as (
    select
        cm_log_owner_id,
        cm_phy_owner_id,
        infection_c,
        inf_add_pl_time,
        inf_add_pl_user_id,
        inf_cmt_pl,
        inf_rsv_pl_time,
        inf_rsv_pl_user_id,
        line,
        pat_id
    from {{ source('clarity_ods', 'hsp_infection_pl') }}

)
select
    cast(sq_hsp_infection_pl.pat_id as varchar(18)) as pat_id,
    cast(sq_hsp_infection_pl.line as bigint) as line,
    cast(sq_hsp_infection_pl.infection_c as bigint) as infection_c,
    cast(
        sq_hsp_infection_pl.inf_add_pl_time as timestamp
    ) as inf_add_pl_time,
    cast(
        sq_hsp_infection_pl.inf_add_pl_user_id as varchar(18)
    ) as inf_add_pl_user_id,
    cast(
        sq_hsp_infection_pl.inf_rsv_pl_time as timestamp
    ) as inf_rsv_pl_time,
    cast(
        sq_hsp_infection_pl.inf_rsv_pl_user_id as varchar(18)
    ) as inf_rsv_pl_user_id,
    cast(
        sq_hsp_infection_pl.cm_phy_owner_id as varchar(25)
    ) as cm_phy_owner_id,
    cast(
        sq_hsp_infection_pl.cm_log_owner_id as varchar(25)
    ) as cm_log_owner_id,
    cast(sq_hsp_infection_pl.inf_cmt_pl as varchar(4000)) as inf_cmt_pl
from sq_hsp_infection_pl
