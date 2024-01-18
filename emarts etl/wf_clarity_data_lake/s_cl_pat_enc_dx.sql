/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_pat_enc_dx",
  "MAPPING_NAME": "m_stg_load_cl_pat_enc_dx",
  "MAPPING_ID": 7916,
  "TARGET_ID": 7605,
  "TARGET_NAME": "s_cl_pat_enc_dx"
}
*/

with dx_edit as (
    select distinct
    pat_enc_csn_id,
    dx_edit_diag_id,
    max(line) over (
        partition by
            pat_enc_csn_id,
                dx_edit_diag_id
    ) as line,
    max(dx_edit_inst) over (
        partition by
            pat_enc_csn_id,
            dx_edit_diag_id
    ) as dx_edit_inst
from
    {{ source('clarity_ods', 'enc_dx_edit_trail') }}
),
ed_event as (
    select distinct
        pat.pat_enc_csn_id,
        min(
                case
                    when evnt.event_type = '95' then evnt.event_time
                end
            ) over (partition by pat.pat_enc_csn_id) as ed_depart
    from
        {{ source('clarity_ods', 'ed_iev_event_info') }} as evnt
        inner join {{ source('clarity_ods', 'ed_iev_pat_info') }} as pat on evnt.event_id = pat.event_id
    where
        evnt.event_type = '95'
),
sq_pat_enc_dx as (
    select
    pat_enc_dx.pat_id,
    pat_enc_dx.pat_enc_date_real,
    pat_enc_dx.line,
    pat_enc_dx.contact_date,
    pat_enc_dx.pat_enc_csn_id,
    pat_enc_dx.dx_id,
    pat_enc_dx.icd9_code,
    pat_enc_dx.annotation,
    pat_enc_dx.dx_qualifier_c,
    pat_enc_dx.primary_dx_yn,
    pat_enc_dx.comments,
    pat_enc_dx.cm_ct_owner_id,
    pat_enc_dx.dx_chronic_yn,
    pat_enc_dx.enc_icd_code,
    pat_enc_dx.dx_stage_id,
    pat_enc_dx.update_date,
    pat_enc_dx.dx_unique,
    pat_enc_dx.dx_ed_yn,
    pat_enc_dx.dx_link_prob_id,
    case
      when pat_enc_dx.dx_ed_yn = 'Y' then 'Y'
      when ed_depart >= dx_edit.dx_edit_inst then 'Y'
      else 'N'
    end
    as derived_dx_ed_yn
from
    {{ source('clarity_ods', 'pat_enc_dx') }} as pat_enc_dx
    left join dx_edit on
        pat_enc_dx.pat_enc_csn_id = dx_edit.pat_enc_csn_id
        and pat_enc_dx.dx_id = dx_edit.dx_edit_diag_id
    left join ed_event on
        ed_event.pat_enc_csn_id = pat_enc_dx.pat_enc_csn_id
)
select
    cast(sq_pat_enc_dx.pat_id as varchar(18)) as pat_id,
    cast(sq_pat_enc_dx.pat_enc_date_real as double) as pat_enc_date_real,
    cast(sq_pat_enc_dx.line as bigint) as line,
    cast(sq_pat_enc_dx.contact_date as timestamp) as contact_date,
    cast(sq_pat_enc_dx.pat_enc_csn_id as bigint) as pat_enc_csn_id,
    cast(sq_pat_enc_dx.dx_id as bigint) as dx_id,
    cast(sq_pat_enc_dx.icd9_code as varchar(10)) as icd9_code,
    cast(sq_pat_enc_dx.annotation as varchar(500)) as annotation,
    cast(sq_pat_enc_dx.dx_qualifier_c as varchar(25)) as dx_qualifier_c,
    cast(sq_pat_enc_dx.primary_dx_yn as char(1)) as primary_dx_yn,
    cast(sq_pat_enc_dx.comments as varchar(1024)) as comments,
    cast(sq_pat_enc_dx.cm_ct_owner_id as varchar(25)) as cm_ct_owner_id,
    cast(sq_pat_enc_dx.dx_chronic_yn as char(1)) as dx_chronic_yn,
    cast(sq_pat_enc_dx.enc_icd_code as varchar(10)) as enc_icd_code,
    cast(sq_pat_enc_dx.dx_stage_id as bigint) as dx_stage_id,
    cast(sq_pat_enc_dx.update_date as timestamp) as update_date,
    cast(sq_pat_enc_dx.dx_unique as varchar(254)) as dx_unique,
    cast(sq_pat_enc_dx.dx_ed_yn as char(1)) as dx_ed_yn,
    cast(sq_pat_enc_dx.derived_dx_ed_yn as char(1)) as derived_dx_ed_yn,
    cast(sq_pat_enc_dx.dx_link_prob_id as bigint) as dx_link_prob_id
from sq_pat_enc_dx
