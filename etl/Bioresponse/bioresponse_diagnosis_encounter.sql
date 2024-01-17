-- TODO: find dates for when diagnoses are entered and make this episodic too?
with dx_enc_info as (-- noqa: PRS,L02
    select
        stg_bioresponse_dxcodes.diagnosis_hierarchy_1,
        diagnosis_encounter_all.patient_key,
        diagnosis_encounter_all.encounter_key,
        diagnosis_encounter_all.encounter_date,
        min(diagnosis_encounter_all.problem_noted_date) as problem_noted_date,
        max(diagnosis_encounter_all.problem_resolved_date) as problem_resolved_date,
        max(diagnosis_encounter_all.visit_primary_ind) as visit_primary_ind,
        max(diagnosis_encounter_all.hsp_acct_admit_primary_ind) as hsp_acct_admit_primary_ind,
        max(diagnosis_encounter_all.problem_list_ind) as problem_list_ind,
        max(diagnosis_encounter_all.ip_admit_primary_ind) as ip_admit_primary_ind,
        max(diagnosis_encounter_all.ed_primary_ind) as ed_primary_ind
    from
        {{ ref('stg_bioresponse_dxcodes') }} as stg_bioresponse_dxcodes
        inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
            on stg_bioresponse_dxcodes.icd10_code = diagnosis_encounter_all.icd10_code
    where
        diagnosis_encounter_all.encounter_date >= {{ var('start_data_date') }}
        and diagnosis_encounter_all.visit_diagnosis_ind = 1
    group by
        stg_bioresponse_dxcodes.diagnosis_hierarchy_1,
        diagnosis_encounter_all.patient_key,
        diagnosis_encounter_all.encounter_key,
        diagnosis_encounter_all.encounter_date
)

select
    dx_enc_info.diagnosis_hierarchy_1,
    dx_enc_info.patient_key,
    dx_enc_info.encounter_key,
    dx_enc_info.encounter_date,
    stg_encounter.encounter_type,
    dx_enc_info.problem_noted_date,
    dx_enc_info.problem_resolved_date,
    case when
        problem_resolved_date is null
        then problem_noted_date::date + stg_bioresponse_infectious_windows.normal_infectious_window
        when problem_resolved_date > problem_noted_date::date + max_infectious_window
        then problem_noted_date + normal_infectious_window
        else problem_resolved_date
    end as estimated_end_date,
    dx_enc_info.visit_primary_ind,
    dx_enc_info.hsp_acct_admit_primary_ind,
    dx_enc_info.problem_list_ind,
    dx_enc_info.ip_admit_primary_ind,
    dx_enc_info.ed_primary_ind,
    case
        when 1 in (
            dx_enc_info.visit_primary_ind,
            dx_enc_info.hsp_acct_admit_primary_ind,
            dx_enc_info.problem_list_ind,
            dx_enc_info.ip_admit_primary_ind,
            dx_enc_info.ed_primary_ind
        ) then 0
        else 1
    end as other_encounter_dx_ind,
    case
        when 1 not in (
            dx_enc_info.visit_primary_ind,
            dx_enc_info.hsp_acct_admit_primary_ind,
            dx_enc_info.ip_admit_primary_ind,
            dx_enc_info.ed_primary_ind
        )
        and dx_enc_info.problem_list_ind = 1
        then 1
        else 0
    end as problem_list_only_ind
from
    dx_enc_info
    inner join {{ ref('stg_bioresponse_infectious_windows') }} as stg_bioresponse_infectious_windows
        on dx_enc_info.diagnosis_hierarchy_1 = stg_bioresponse_infectious_windows.diagnosis_hierarchy_1
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on dx_enc_info.encounter_key = stg_encounter.encounter_key
