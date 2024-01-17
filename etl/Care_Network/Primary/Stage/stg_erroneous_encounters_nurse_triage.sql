{{ config(meta = {
    'critical': true
}) }}

select
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    stg_encounter.encounter_type_id
from {{ ref('stg_encounter') }} as stg_encounter
    left join {{ source('clarity_ods', 'pat_call_disp') }} as pat_call_disp
        on stg_encounter.csn = pat_call_disp.pat_enc_csn_id
    left join {{ source('clarity_ods', 'pat_enc_rsn_visit') }} as pat_enc_rsn_visit
        on stg_encounter.csn = pat_enc_rsn_visit.pat_enc_csn_id
    left join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on stg_encounter.encounter_key = diagnosis_encounter_all.encounter_key
where stg_encounter.encounter_date >= '2018-07-01'
    and stg_encounter.encounter_type_id in (70, 71) -- telephone, nurse triage
    and (lower(stg_encounter.intended_use_name) = 'primary care'
        -- CENTRAL TRIAGE PC, PC OFFCE HRS RN TRIAGE, 'CALL CENTER(AFTER H*'
        or stg_encounter.department_id in (82, 101001178, 37))
    -- disposition name = 'ERRONEOUS ENCOUNTER'
    and (pat_call_disp.phone_disp_c = 85
        --rsn_nm not like 'ERRONEOUS ENCOUNTER-DISREGARD'
        or pat_enc_rsn_visit.enc_reason_id = 10000
        or diagnosis_encounter_all.diagnosis_id in (
            128113, -- diagnosis_name = 'ERRONEOUS ENCOUNTER--DISREGARD'
            15079, -- diagnosis_name = 'ERRONEOUS ENCOUNTER--DISREGARD'
            1065517 -- diagnosis_name = 'Erroneous Encounter Code'
            )
        )
group by
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    stg_encounter.encounter_type_id
