{{ config(meta = {
    'critical': true
}) }}

with disposition_details as (
-- For the disposition of 'See within 3 Days in Office' with additional details:
-- 'Triager thinks child needs to be seen for non-urgent problem'
-- 'Caller wants child seen for non-urgent problem'
select
    stg_encounter_nurse_triage.encounter_key,
    lpq_prompt_txt.free_text
from {{ ref('stg_encounter_nurse_triage') }} as stg_encounter_nurse_triage
    inner join
        {{ source('clarity_ods', 'pat_enc_call_prpts') }} as pat_enc_call_prpts
        on stg_encounter_nurse_triage.csn = pat_enc_call_prpts.pat_enc_csn_id
    inner join {{ source('clarity_ods', 'lpq_prompt_txt') }} as lpq_prompt_txt
        on pat_enc_call_prpts.clicked_lpqs = lpq_prompt_txt.prompt_id
where pat_enc_call_prpts.clicked_lpqs in (387899, 387901)
),
protocol_disposition as (
select
    stg_encounter_nurse_triage.encounter_key,
    pat_call_disp.line_count as seq_num,
    pat_call_disp.phone_disp_time,
    pat_call_disp.phone_disp_cmt,
    zc_phone_disp.phone_disp_c as disposition_id,
    zc_phone_disp.name as disposition_nm,
    case
        when pat_call_disp.phone_disp_suggst_c = 1 then 'Suggested'
        when pat_call_disp.phone_disp_suggst_c = 2 then 'Manually Entered'
        when pat_call_disp.phone_disp_suggst_c = 3 then 'Suggested and Manual'
        else null
    end as disp_suggest_cat,
    zc_phone_disp_original.phone_disp_c as original_disposition_id,
    zc_phone_disp_original.name as original_disposition_nm,
    nurse_triage_ptcl.nurse_triage_ptcl_id,
    nurse_triage_ptcl.nurse_triage_ptcl_nm as nurse_triage_ptcl_nm,
    clarity_ser.prov_id as disposition_prov_id,
    clarity_ser.prov_name as disposition_prov_name
from {{ ref('stg_encounter_nurse_triage') }} as stg_encounter_nurse_triage
    inner join {{ source('clarity_ods', 'pat_call_disp') }} as pat_call_disp
        on pat_call_disp.pat_enc_csn_id = stg_encounter_nurse_triage.csn
    inner join {{ source('clarity_ods', 'zc_phone_disp') }} as zc_phone_disp
        on zc_phone_disp.phone_disp_c = pat_call_disp.phone_disp_c
    left join {{ source('clarity_ods', 'zc_phone_disp') }} as zc_phone_disp_original
        on zc_phone_disp_original.phone_disp_c = pat_call_disp.original_disp_c
    -- replace with clarity_protocol when moved to appropriate security group
    left join {{ source('cdw', 'nurse_triage_ptcl') }} as nurse_triage_ptcl
        on nurse_triage_ptcl.nurse_triage_ptcl_id = pat_call_disp.suggest_protocol_id
    left join {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser
        on clarity_ser.user_id = pat_call_disp.phone_disp_user_id
)
select
    stg_encounter_nurse_triage.encounter_key,
    stg_encounter_nurse_triage.visit_key,
    stg_encounter_nurse_triage.csn,
    case
        when protocol_disposition.encounter_key is null then 1
        else protocol_disposition.seq_num
    end as seq_num,
    protocol_disposition.disposition_prov_id,
    protocol_disposition.disposition_prov_name,
    protocol_disposition.nurse_triage_ptcl_id,
    case
        when protocol_disposition.encounter_key is null
                or (protocol_disposition.nurse_triage_ptcl_id = 0
                    and protocol_disposition.seq_num = 1)
            then 'No triage protocol selected'
        when (protocol_disposition.seq_num > 1
                and protocol_disposition.nurse_triage_ptcl_id = 0)
            then 'N/A - See previous protocol'
        else protocol_disposition.nurse_triage_ptcl_nm
    end as nurse_triage_ptcl_nm,
    protocol_disposition.disposition_id,
    case when protocol_disposition.encounter_key is null
        then 'No disposition'
        else protocol_disposition.disposition_nm
    end as disposition_name,
    protocol_disposition.phone_disp_time,
    protocol_disposition.disp_suggest_cat,
    protocol_disposition.original_disposition_id,
    protocol_disposition.original_disposition_nm,
    protocol_disposition.phone_disp_cmt,
    disposition_details.free_text
from {{ ref('stg_encounter_nurse_triage') }} as stg_encounter_nurse_triage
    left join protocol_disposition
        on stg_encounter_nurse_triage.encounter_key = protocol_disposition.encounter_key
    left join disposition_details
        on disposition_details.encounter_key = stg_encounter_nurse_triage.encounter_key
            -- only for 'See within 3 Days in Office'
            and protocol_disposition.disposition_id = 111
group by
    stg_encounter_nurse_triage.encounter_key,
    protocol_disposition.encounter_key,
    stg_encounter_nurse_triage.visit_key,
    stg_encounter_nurse_triage.csn,
    seq_num,
    protocol_disposition.disposition_prov_id,
    protocol_disposition.disposition_prov_name,
    protocol_disposition.nurse_triage_ptcl_id,
    nurse_triage_ptcl_nm,
    protocol_disposition.disposition_id,
    disposition_name,
    protocol_disposition.phone_disp_time,
    protocol_disposition.disp_suggest_cat,
    protocol_disposition.original_disposition_id,
    protocol_disposition.original_disposition_nm,
    protocol_disposition.phone_disp_cmt,
    disposition_details.free_text
