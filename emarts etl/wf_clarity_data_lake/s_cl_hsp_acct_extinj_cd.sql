/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_hsp_acct_extinj_cd",
  "MAPPING_NAME": "m_stg_load_cl_hsp_acct_extinj_cd",
  "MAPPING_ID": 7913,
  "TARGET_ID": 7598,
  "TARGET_NAME": "s_cl_hsp_acct_extinj_cd"
}
*/
select
    cast(hsp_acct_extinj_cd.cm_log_owner_id as varchar(25)) as cm_log_owner_id,
    cast(hsp_acct_extinj_cd.cm_phy_owner_id as varchar(25)) as cm_phy_owner_id,
    cast(hsp_acct_extinj_cd.ecd_dx_afct_rom_yn as char(1)) as ecd_dx_afct_rom_yn,
    cast(hsp_acct_extinj_cd.ecd_dx_afct_soi_yn as char(1)) as ecd_dx_afct_soi_yn,
    cast(hsp_acct_extinj_cd.ecd_hac_yn as char(1)) as ecd_hac_yn,
    cast(hsp_acct_extinj_cd.ecode_dx_excld_yn as char(1)) as ecode_dx_excld_yn,
    cast(hsp_acct_extinj_cd.ecode_dx_poa_c as bigint) as ecode_dx_poa_c,
    cast(hsp_acct_extinj_cd.ecode_dx_rom_c as bigint) as ecode_dx_rom_c,
    cast(hsp_acct_extinj_cd.ecode_dx_soi_c as bigint) as ecode_dx_soi_c,
    cast(hsp_acct_extinj_cd.ext_comorbidity_c as bigint) as ext_comorbidity_c,
    cast(hsp_acct_extinj_cd.ext_comorbidity_yn as varchar(254)) as ext_comorbidity_yn,
    cast(hsp_acct_extinj_cd.ext_dx_aff_drg_yn as varchar(254)) as ext_dx_aff_drg_yn,
    cast(hsp_acct_extinj_cd.ext_injury_dx_id as bigint) as ext_injury_dx_id,
    cast(hsp_acct_extinj_cd.ext_injury_poa_ynu as varchar(254)) as ext_injury_poa_ynu,
    cast(hsp_acct_extinj_cd.hsp_account_id as bigint) as hsp_account_id,
    cast(hsp_acct_extinj_cd.line as bigint) as line
from {{ source('clarity_ods', 'hsp_acct_extinj_cd') }} as hsp_acct_extinj_cd
