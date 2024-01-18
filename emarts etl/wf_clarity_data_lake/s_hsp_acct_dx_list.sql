/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_hsp_acct_dx_list",
  "MAPPING_NAME": "m_stg_load_cl_hsp_acct_dx_list",
  "MAPPING_ID": 7903,
  "TARGET_ID": 7637,
  "TARGET_NAME": "s_hsp_acct_dx_list"
}
*/

with
sq_hsp_acct_dx_list as (
    select
        hsp_acct_dx_list.hsp_account_id,
        hsp_acct_dx_list.line,
        hsp_acct_dx_list.dx_id,
        hsp_acct_dx_list.cm_phy_owner_id,
        hsp_acct_dx_list.cm_log_owner_id,
        zc_dx_poa.name as dx_poa_ynu,
        hsp_acct_dx_list.dx_affects_drg_yn,
        hsp_acct_dx_list.dx_comorbidity_yn,
        hsp_acct_dx_list.final_dx_excld_yn,
        hsp_acct_dx_list.final_dx_soi_c,
        hsp_acct_dx_list.final_dx_rom_c,
        hsp_acct_dx_list.fnl_dx_afct_soi_yn,
        hsp_acct_dx_list.fnl_dx_afct_rom_yn,
        hsp_acct_dx_list.final_dx_poa_c,
        hsp_acct_dx_list.dx_comorbidity_c,
        hsp_acct_dx_list.dx_hac_yn
    from
        {{ source('clarity_ods', 'hsp_acct_dx_list') }} as hsp_acct_dx_list
        left outer join {{ source('clarity_ods', 'zc_dx_poa') }} as zc_dx_poa
            on hsp_acct_dx_list.final_dx_poa_c = zc_dx_poa.dx_poa_c
)
select
    cast(sq_hsp_acct_dx_list.hsp_account_id as bigint) as hsp_account_id,
    cast(sq_hsp_acct_dx_list.line as bigint) as line,
    cast(sq_hsp_acct_dx_list.dx_id as bigint) as dx_id,
    cast(sq_hsp_acct_dx_list.cm_phy_owner_id as varchar(25)) as cm_phy_owner_id,
    cast(sq_hsp_acct_dx_list.cm_log_owner_id as varchar(25)) as cm_log_owner_id,
    cast(sq_hsp_acct_dx_list.dx_poa_ynu as varchar(254)) as dx_poa_ynu,
    cast(sq_hsp_acct_dx_list.dx_affects_drg_yn as varchar(254)) as dx_affects_drg_yn,
    cast(sq_hsp_acct_dx_list.dx_comorbidity_yn as varchar(254)) as dx_comorbidity_yn,
    cast(sq_hsp_acct_dx_list.final_dx_excld_yn as char(1)) as final_dx_excld_yn,
    cast(sq_hsp_acct_dx_list.final_dx_soi_c as bigint) as final_dx_soi_c,
    cast(sq_hsp_acct_dx_list.final_dx_rom_c as bigint) as final_dx_rom_c,
    cast(sq_hsp_acct_dx_list.fnl_dx_afct_soi_yn as char(1)) as fnl_dx_afct_soi_yn,
    cast(sq_hsp_acct_dx_list.fnl_dx_afct_rom_yn as char(1)) as fnl_dx_afct_rom_yn,
    cast(sq_hsp_acct_dx_list.final_dx_poa_c as bigint) as final_dx_poa_c,
    cast(sq_hsp_acct_dx_list.dx_comorbidity_c as bigint) as dx_comorbidity_c,
    cast(sq_hsp_acct_dx_list.dx_hac_yn as char(1)) as dx_hac_yn
from sq_hsp_acct_dx_list
