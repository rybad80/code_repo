{{ config(
    materialized='table',
    dist='encounter_key',
    meta = {
        'critical': true
    }
) }}

select
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    stg_dx_all_combos.dx_key,
    {{
        dbt_utils.surrogate_key([
            'stg_dx_all_combos.diagnosis_id',
            "'CLARITY'"
        ])
    }} as diagnosis_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_dx_all_combos.diagnosis_name,
    stg_dx_all_combos.icd10_code,
    stg_dx_all_combos.icd9_code,
    trim( --noqa: PRS
        trailing ', ' from case
            when stg_dx_visit_diagnosis.encounter_key is not null then 'visit_diagnosis, ' else ''
        end
        || case when stg_dx_problem_list.encounter_key is not null then 'problem_list, ' else '' end
        || case when stg_dx_pb_transaction.encounter_key is not null then 'pb_transaction' else '' end
    ) as source_summary,
    case when stg_dx_visit_diagnosis.encounter_key is not null then 1 else 0 end as visit_diagnosis_ind,
    stg_dx_visit_diagnosis.visit_diagnosis_seq_num,
    case when
        coalesce(stg_dx_visit_diagnosis.ed_primary_ind, 0) = 1
        or coalesce(stg_dx_visit_diagnosis.homecare_primary_ind, 0) = 1
        or coalesce(stg_dx_visit_diagnosis.hsp_acct_admit_primary_ind, 0) = 1
        or coalesce(stg_dx_visit_diagnosis.hsp_acct_final_primary_ind, 0) = 1
        or coalesce(stg_dx_visit_diagnosis.ip_admit_primary_ind, 0) = 1
        or coalesce(stg_dx_visit_diagnosis.visit_primary_ind, 0) = 1
            then 1
        else 0
    end as marked_primary_ind,
    coalesce(stg_dx_visit_diagnosis.ed_primary_ind, 0) as ed_primary_ind,
    coalesce(stg_dx_visit_diagnosis.ed_other_ind, 0) as ed_other_ind,
    coalesce(stg_dx_visit_diagnosis.homecare_primary_ind, 0) as homecare_primary_ind,
    coalesce(stg_dx_visit_diagnosis.homecare_other_ind, 0) as homecare_other_ind,
    coalesce(stg_dx_visit_diagnosis.hsp_acct_admit_primary_ind, 0) as hsp_acct_admit_primary_ind,
    coalesce(stg_dx_visit_diagnosis.hsp_acct_admit_other_ind, 0) as hsp_acct_admit_other_ind,
    coalesce(stg_dx_visit_diagnosis.hsp_acct_final_primary_ind, 0) as hsp_acct_final_primary_ind,
    coalesce(stg_dx_visit_diagnosis.hsp_acct_final_other_ind, 0) as hsp_acct_final_other_ind,
    coalesce(stg_dx_visit_diagnosis.ip_admit_primary_ind, 0) as ip_admit_primary_ind,
    coalesce(stg_dx_visit_diagnosis.ip_admit_other_ind, 0) as ip_admit_other_ind,
    coalesce(stg_dx_visit_diagnosis.visit_primary_ind, 0) as visit_primary_ind,
    coalesce(stg_dx_visit_diagnosis.visit_other_ind, 0) as visit_other_ind,
    case when stg_dx_problem_list.encounter_key is not null then 1 else 0 end as problem_list_ind,
    stg_dx_problem_list.noted_date as problem_noted_date,
    stg_dx_problem_list.resolved_date as problem_resolved_date,
    case when stg_dx_pb_transaction.encounter_key is not null then 1 else 0 end as pb_transaction_ind,
    stg_dx_all_combos.diagnosis_id,
    stg_dx_all_combos.external_diagnosis_id,
    stg_encounter.pat_key,
    stg_encounter.patient_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    {{ref('stg_dx_all_combos')}} as stg_dx_all_combos
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.encounter_key = stg_dx_all_combos.encounter_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_dx_visit_diagnosis')}} as stg_dx_visit_diagnosis
        on stg_dx_visit_diagnosis.encounter_key = stg_dx_all_combos.encounter_key
            and stg_dx_visit_diagnosis.dx_id = stg_dx_all_combos.diagnosis_id
    left join {{ref('stg_dx_problem_list')}} as stg_dx_problem_list
        on stg_dx_problem_list.encounter_key = stg_dx_all_combos.encounter_key
            and stg_dx_problem_list.dx_id = stg_dx_all_combos.diagnosis_id
    left join {{ref('stg_dx_pb_transaction')}} as stg_dx_pb_transaction
        on stg_dx_pb_transaction.encounter_key = stg_dx_all_combos.encounter_key
            and stg_dx_pb_transaction.dx_id = stg_dx_all_combos.diagnosis_id
            and stg_dx_pb_transaction.enc_dx_row_num = 1
