{{ config(materialized='table', dist='encounter_key') }}

select
    stg_encounter.encounter_key,
    stg_dx_visit_diagnosis_long.pat_enc_csn_id,
    stg_dx_visit_diagnosis_long.dx_id,
    min(stg_dx_visit_diagnosis_long.line) as visit_diagnosis_seq_num,
    max(case when src = 'PAT_ENC_DX' and dx_status = 'ED Primary' then 1 else 0 end) as ed_primary_ind,
    max(case when src in ('PAT_ENC_DX', 'WELLSOFT') and dx_status = 'ED Other' then 1 else 0 end) as ed_other_ind,
    max(case when src = 'FASTRACK' and dx_status = 'Visit Primary' then 1 else 0 end) as homecare_primary_ind,
    max(case when src = 'FASTRACK' and dx_status = 'Visit Other' then 1 else 0 end) as homecare_other_ind,
    max(case when src = 'HSP_ACCT_ADMIT_DX' and line = 1 then 1 else 0 end) as hsp_acct_admit_primary_ind,
    max(case when src = 'HSP_ACCT_ADMIT_DX' and line != 1 then 1 else 0 end) as hsp_acct_admit_other_ind,
    max(case when src = 'HSP_ACCT_DX_LIST' and line = 1 then 1 else 0 end) as hsp_acct_final_primary_ind,
    max(case
        when src = 'HSP_ACCT_DX_LIST' and line != 1 then 1
        when src = 'HSP_ACCT_EXTINJ_CD' then 1
        else 0
    end) as hsp_acct_final_other_ind,
    max(case when src = 'HSP_ADMIT_DIAG' and line = 1 then 1 else 0 end) as ip_admit_primary_ind,
    max(case when src = 'HSP_ADMIT_DIAG' and line != 1 then 1 else 0 end) as ip_admit_other_ind,
    max(
        case when src in ('PAT_ENC_DX', 'IDX') and dx_status = 'Visit Primary' then 1 else 0 end
    ) as visit_primary_ind,
    max(case when src in ('PAT_ENC_DX', 'IDX') and dx_status = 'Visit Other' then 1 else 0 end) as visit_other_ind
from
    {{ref('stg_dx_visit_diagnosis_long')}} as stg_dx_visit_diagnosis_long
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = stg_dx_visit_diagnosis_long.pat_enc_csn_id
group by
    stg_encounter.encounter_key,
    stg_dx_visit_diagnosis_long.pat_enc_csn_id,
    stg_dx_visit_diagnosis_long.dx_id
