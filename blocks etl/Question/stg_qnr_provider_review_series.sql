--PROs Provider Review for Series Assigned Encounters 

with post_encounter as (--start region

    select
        questionnaire_patient_assigned.visit_key,
        stg_encounter.pat_key,
        questionnaire_patient_assigned.encounter_date,
        stg_encounter.visit_key as review_visit_key,
        stg_encounter.encounter_date as review_encounter_date,
        hno_info.letter_template_smarttext_id,
        hno_info_2.starting_smartphrase_id,
        row_number() over (
            partition by questionnaire_patient_assigned.visit_key
            order by stg_encounter.encounter_date asc
        ) as provider_review_order

    from {{ ref('questionnaire_patient_assigned') }} as questionnaire_patient_assigned
        inner join {{ ref('lookup_pro_questionnaire') }} as lookup_pro_questionnaire
            on questionnaire_patient_assigned.form_id = lookup_pro_questionnaire.form_id
        inner join {{ ref ('stg_encounter') }} as stg_encounter
            on questionnaire_patient_assigned.pat_key = stg_encounter.pat_key
        inner join {{source('clarity_ods', 'hno_info')}} as hno_info
            on stg_encounter.csn = hno_info.pat_enc_csn_id
        left join {{source('clarity_ods', 'hno_info_2')}} as hno_info_2
            on hno_info.note_id = hno_info_2.note_id

    where
        stg_encounter.encounter_date > questionnaire_patient_assigned.assigned_qnr_start_date
        --Relevant SmartPhrase and SmartText for Series, created by Tina Fisher from PROs team 
        and (hno_info_2.starting_smartphrase_id = 326465
            or hno_info.letter_template_smarttext_id = 37507)
        and lookup_pro_questionnaire.series_provider_doc_ind = 1

    group by
        questionnaire_patient_assigned.visit_key,
        stg_encounter.pat_key,
        questionnaire_patient_assigned.encounter_date,
        review_visit_key,
        review_encounter_date,
        hno_info.letter_template_smarttext_id,
        hno_info_2.starting_smartphrase_id

)--end region 

select
    post_encounter.visit_key,
    post_encounter.encounter_date,
    post_encounter.pat_key,
    post_encounter.review_visit_key,
    post_encounter.review_encounter_date,
    post_encounter.letter_template_smarttext_id,
    post_encounter.starting_smartphrase_id,
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
    1 as post_review_ind

from post_encounter
    inner join {{ ref('stg_qnr_provider_review') }} as stg_qnr_provider_review
        on post_encounter.review_visit_key = stg_qnr_provider_review.visit_key

where
    provider_review_order = 1

group by
    post_encounter.visit_key,
    post_encounter.encounter_date,
    post_encounter.pat_key,
    post_encounter.review_visit_key,
    post_encounter.review_encounter_date,
    post_encounter.letter_template_smarttext_id,
    post_encounter.starting_smartphrase_id,
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
