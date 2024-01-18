with rcap as (
    select
        redcap_detail.record,
        master_redcap_question.field_nm,
        master_redcap_question.element_label,
        master_redcap_question.element_type,
        -- shorten field values for next pivot step
        -- failure to complete this step causes buffer issues when pivoting
        lower(
            substr(
                coalesce(
                    master_redcap_element_answr.element_desc,
                    redcap_detail.value
                ),
                1,
                200
            )
        )::varchar(200) as response
    from
        {{ source('cdw', 'redcap_detail') }} as redcap_detail
        left join {{ source('cdw', 'master_redcap_project') }} as master_redcap_project
            on redcap_detail.mstr_project_key = master_redcap_project.mstr_project_key
        left join {{ source('cdw', 'master_redcap_question') }} as master_redcap_question
            on redcap_detail.mstr_redcap_quest_key = master_redcap_question.mstr_redcap_quest_key
        left join {{ source('cdw', 'master_redcap_element_answr') }} as master_redcap_element_answr
            on redcap_detail.mstr_redcap_quest_key = master_redcap_element_answr.mstr_redcap_quest_key
            and redcap_detail.value = master_redcap_element_answr.element_id
    where
        redcap_detail.cur_rec_ind = 1
        and master_redcap_project.project_id = 309
)

select
    rcap.record,
    rcap.field_nm,
    rcap.element_label,
    rcap.element_type,
    rcap.response,
    case
        when rcap.field_nm = 'airway_support_type'
            and rcap.response = 'bag-valve-mask'
            then 'airway_support_bag_valve_mask_ind'
        when rcap.field_nm = 'airway_support_type'
            and rcap.response = 'cpap'
            then 'airway_support_cpap_ind'
        when rcap.field_nm = 'airway_support_type'
            and rcap.response = 'bipap'
            then 'airway_support_bipap_ind'
        when rcap.field_nm = 'airway_support_type'
            and rcap.response = 'intubation'
            then 'airway_support_intubation_ind'
        when rcap.field_nm = 'airway_support_type'
            and rcap.response = 'bag-valve-artificial airway'
            then 'airway_support_bag_valve_artificial_airway_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'epinephrine im'
            then 'meds_epinephrine_im_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'epinephrine iv'
            then 'meds_epinephrine_iv_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'atropine iv'
            then 'meds_atropine_iv_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'sodium bicarbonate iv'
            then 'meds_sodium_bicarbonate_iv_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'sedative/narcotic (midazolam'
            then 'meds_sedative_narcotic_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'nmb (vecuronium'
            then 'meds_nmb_vecuronium_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'fluid bolus'
            then 'meds_fluid_bolus_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'blood products'
            then 'meds_blood_products_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'anti-convulsant(s)'
            then 'meds_anti_convulsants_ind'
        when rcap.field_nm = 'meds_type'
            and rcap.response = 'other (explain below)'
            then 'meds_other_ind'
        when rcap.field_nm = 'code_int'
            and rcap.response = 'labs'
            then 'code_int_labs_ind'
        when rcap.field_nm = 'code_int'
            and rcap.response = 'needle decompression'
            then 'code_int_needle_decompr_ind'
        when rcap.field_nm = 'code_int'
            and rcap.response = 'echo'
            then 'code_int_echo_ind'
        when rcap.field_nm = 'code_int'
            and rcap.response = 'd-stick'
            then 'code_int_d_stick_ind'
        when rcap.field_nm = 'code_int'
            and rcap.response = 'other'
            then 'code_int_other_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'general staff concern/gut feeling'
            then 'reason_staff_concern_gut_feeling_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'worrisome respiratory change'
            then 'reason_respiratory_change_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'worrisome cardiovascular change'
            then 'reason_cardiovascular_change_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'worrisome neurologic change (i.e. change in loc)'
            then 'reason_neurologic_change_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'family concern'
            then 'reason_family_concern_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'mpews'
            then 'reason_mpews_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'vascular access'
            then 'reason_vascular_access_ind'
        when rcap.field_nm = 'reason'
            and rcap.response = 'other'
            then 'reason_other_ind'
        when rcap.field_nm = 'neuro_interventions'
            and rcap.response = 'altered mental status'
            then 'neuro_interventions_alt_mental_status_ind'
        when rcap.field_nm = 'neuro_interventions'
            and rcap.response = 'concerns for elevated icp/shunt malfunction'
            then 'neuro_interventions_concern_icp_shunt_malfunc_ind'
        when rcap.field_nm = 'neuro_interventions'
            and rcap.response = 'increase seizure frequency or status epilepticus'
            then 'neuro_interventions_increase_seizure_freq_ind'
        when rcap.field_nm = 'neuro_interventions'
            and rcap.response = 'other'
            then 'neuro_interventions_other_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'als protocol'
            then 'cqi_cat_als_protocol_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'ecg rhythm analysis'
            then 'cqi_cat_ecg_rhythm_analysis_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'equipment'
            then 'cqi_cat_equipment_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'family support'
            then 'cqi_cat_family_support_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'leadership'
            then 'cqi_cat_leadership_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'medications'
            then 'cqi_cat_medications_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'notification'
            then 'cqi_cat_notification_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'roles'
            then 'cqi_cat_roles_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'universal precautions'
            then 'cqi_cat_universal_precautions_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'ascom'
            then 'cqi_cat_ascom_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'physicians did not know team was coming'
            then 'cqi_cat_md_unaware_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'nurses did not know team was coming'
            then 'cqi_cat_rn_unaware_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'cat recommendations not followed by primary team'
            then 'cqi_cat_cat_recommendations_not_followed_ind'
        when rcap.field_nm = 'cqi_cat'
            and rcap.response = 'unplanned extubation'
            then 'cqi_cat_unplanned_extubation_ind'
        when rcap.field_nm = 'codenarrator_barrier'
            and rcap.response = 'not yet officially implemented within unit'
            then 'codenarrator_barrier_not_impl_in_unit_ind'
        when rcap.field_nm = 'codenarrator_barrier'
            and rcap.response = 'computer access limited'
            then 'codenarrator_barrier_computer_access_ind'
        when rcap.field_nm = 'codenarrator_barrier'
            and rcap.response = 'computer malfunction/technology issue'
            then 'codenarrator_barrier_tech_issue_ind'
        when rcap.field_nm = 'codenarrator_barrier'
            and rcap.response = 'epic issue (i.e. downtime or trouble logging in)'
            then 'codenarrator_barrier_epic_issue_ind'
        when rcap.field_nm = 'codenarrator_barrier'
            and rcap.response = 'staff comfort'
            then 'codenarrator_barrier_staff_comfort_ind'
        when rcap.field_nm = 'codenarrator_barrier'
            and rcap.response = 'other'
            then 'codenarrator_barrier_other_ind'
        else rcap.field_nm
    end as recode_field_nm,
    case
        when rcap.element_type = 'checkbox'
        and rcap.field_nm in (
            'airway_support_type',
            'meds_type',
            'reason',
            'neuro_interventions',
            'cqi_cat',
            'code_int',
            'codenarrator_barrier'
        )
            then '1'
        else rcap.response
    end as recode_response
from
    rcap
