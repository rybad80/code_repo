select
    stg_diagnosis_medically_complex.visit_key,
    stg_diagnosis_medically_complex.encounter_key,
    stg_diagnosis_medically_complex.patient_name,
    stg_diagnosis_medically_complex.mrn,
    stg_diagnosis_medically_complex.dob,
    stg_diagnosis_medically_complex.csn,
    stg_diagnosis_medically_complex.encounter_date,
    stg_diagnosis_medically_complex.tech_dependent_ind,
    stg_diagnosis_medically_complex.hematu_ccc_ind,
    stg_diagnosis_medically_complex.renal_ccc_ind,
    stg_diagnosis_medically_complex.gi_ccc_ind,
    stg_diagnosis_medically_complex.malignancy_ccc_ind,
    stg_diagnosis_medically_complex.metabolic_ccc_ind,
    stg_diagnosis_medically_complex.neonatal_ccc_ind,
    stg_diagnosis_medically_complex.congeni_genetic_ccc_ind,
    stg_diagnosis_medically_complex.resp_ccc_ind,
    stg_diagnosis_medically_complex.cvd_ccc_ind,
    stg_diagnosis_medically_complex.neuromusc_ccc_ind,
    stg_diagnosis_medically_complex.complex_chronic_condition_ind,
    stg_diagnosis_medically_complex.medically_complex_ind,
    stg_diagnosis_medically_complex.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    {{ref('stg_diagnosis_medically_complex')}} as stg_diagnosis_medically_complex
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_diagnosis_medically_complex.encounter_key
