--PROs Provider Review for Non-Series Assigned Encounters 

select
    questionnaire_patient_assigned.visit_key,
    questionnaire_patient_assigned.encounter_date,
    questionnaire_patient_assigned.pat_key,
    null as review_visit_key,
    null as review_encounter_date,
    null as letter_template_smarttext_id,
    null as starting_smartphrase_id,
    stg_qnr_provider_review.concept_id,
    stg_qnr_provider_review.concept_desc,
    stg_qnr_provider_review.sde_entered_employee,
    stg_qnr_provider_review.sde_entered_date,
    stg_qnr_provider_review.sde_value,
    stg_qnr_provider_review.sde_use_ind,
    stg_qnr_provider_review.sde_0_ind,
    stg_qnr_provider_review.sde_1_ind,
    stg_qnr_provider_review.sde_2_ind,
    stg_qnr_provider_review.sde_3_ind,
    0 as post_review_ind

from {{ ref('questionnaire_patient_assigned') }} as questionnaire_patient_assigned
    inner join
        {{ ref('stg_qnr_provider_review') }} as stg_qnr_provider_review on
            questionnaire_patient_assigned.visit_key = stg_qnr_provider_review.visit_key

group by
    questionnaire_patient_assigned.visit_key,
    questionnaire_patient_assigned.encounter_date,
    questionnaire_patient_assigned.pat_key,
    review_visit_key,
    review_encounter_date,
    letter_template_smarttext_id,
    starting_smartphrase_id,
    stg_qnr_provider_review.concept_id,
    stg_qnr_provider_review.concept_desc,
    stg_qnr_provider_review.sde_entered_employee,
    stg_qnr_provider_review.sde_entered_date,
    stg_qnr_provider_review.sde_value,
    stg_qnr_provider_review.sde_use_ind,
    stg_qnr_provider_review.sde_0_ind,
    stg_qnr_provider_review.sde_1_ind,
    stg_qnr_provider_review.sde_2_ind,
    stg_qnr_provider_review.sde_3_ind,
    post_review_ind
