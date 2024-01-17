{%- set fields = [
    'aa_category',
    'account_number',
    'admit_source',
    'attending_saw_cat',
    'airway_support_bag_valve_artificial_airway_ind',
    'airway_support_bag_valve_mask_ind',
    'airway_support_bipap_ind',
    'airway_support_cpap_ind',
    'airway_support_intubation_ind',
    'caller_role2',
    'cat_calledd',
    'cat_exist_nippv',
    'cat_fu',
    'cat_none_reason',
    'cat_provider',
    'cat_rnfu',
    'cc_done',
    'cc_duration',
    'cc_oneminute',
    'cha_code',
    'code_category',
    'code_int_d_stick_ind',
    'code_int_echo_ind',
    'code_int_labs_ind',
    'code_int_needle_decompr_ind',
    'code_int_other_ind',
    'codenarrator_barrier_computer_access_ind',
    'codenarrator_barrier_epic_issue_ind',
    'codenarrator_barrier_not_impl_in_unit_ind',
    'codenarrator_barrier_other_ind',
    'codenarrator_barrier_staff_comfort_ind',
    'codenarrator_barrier_tech_issue_ind',
    'codenarrator_use',
    'cpa_arc',
    'cqi_cat_als_protocol_ind',
    'cqi_cat_ascom_ind',
    'cqi_cat_cat_recommendations_not_followed_ind',
    'cqi_cat_ecg_rhythm_analysis_ind',
    'cqi_cat_equipment_ind',
    'cqi_cat_family_support_ind',
    'cqi_cat_leadership_ind',
    'cqi_cat_md_unaware_ind',
    'cqi_cat_medications_ind',
    'cqi_cat_notification_ind',
    'cqi_cat_rn_unaware_ind',
    'cqi_cat_roles_ind',
    'cqi_cat_universal_precautions_ind',
    'cqi_cat_unplanned_extubation_ind',
    'cqi_form',
    'cqi_issues',
    'dcp',
    'debrief_cat',
    'debriefed',
    'disposition',
    'dnr_event',
    'dob',
    'documentation_probs',
    'dx',
    'edu_remediation_complete',
    'escalated_to_code',
    'event_dt_tm',
    'event_location_details',
    'event_location',
    'event_type',
    'first_name',
    'followup_pend',
    'icu_cpa_dt_tm',
    'icu_cpa',
    'keypoint_summary',
    'last_name',
    'meds_anti_convulsants_ind',
    'meds_atropine_iv_ind',
    'meds_blood_products_ind',
    'meds_blood',
    'meds_epinephrine_im_ind',
    'meds_epinephrine_iv_ind',
    'meds_fluid_bolus_ind',
    'meds_nmb_vecuronium_ind',
    'meds_other_ind',
    'meds_sedative_narcotic_ind',
    'meds_sodium_bicarbonate_iv_ind',
    'mrn',
    'neuro_interventions_alt_mental_status_ind',
    'neuro_interventions_concern_icp_shunt_malfunc_ind',
    'neuro_interventions_increase_seizure_freq_ind',
    'neuro_interventions_other_ind',
    'non_patient_category',
    'nonicuinpt',
    'nonptescalation',
    'reason_cardiovascular_change_ind',
    'reason_family_concern_ind',
    'reason_mpews_ind',
    'reason_neurologic_change_ind',
    'reason_other_ind',
    'reason_respiratory_change_ind',
    'reason_staff_concern_gut_feeling_ind',
    'reason_vascular_access_ind',
    'sex',
    'sig_complete_leader',
    'sig_complete',
    'survival_status',
    'telecat_time',
    'telecat_use',
    'timecatleft',
    'timecatreturn',
    'timecbtret2_7a82_8382_8b1',
    'timefuleave',
    'timefureturn',
    'timenprtret2_7a82_838',
    'video'
] -%}

with record_id_distinct as (
    select
        pcoti_redcap_raw.record
    from
        {{ ref('stg_pcoti_redcap_raw') }} as pcoti_redcap_raw
    group by
        pcoti_redcap_raw.record
),

-- can't use dbt_utils.pivot because not all question response values are used,
-- but we want all columns to appear in final dataset even if they're empty
pivot_fields as (
    select
        pcoti_redcap_raw.record,
        {% for field in fields %}
        max(
            case
                when pcoti_redcap_raw.recode_field_nm = '{{ field }}' then pcoti_redcap_raw.recode_response
            end
        ) as {{ field }}
        {% if not loop.last %},{% endif %} -- noqa: L019
        {% endfor %}
    from
        {{ ref('stg_pcoti_redcap_raw') }} as pcoti_redcap_raw
    group by
        pcoti_redcap_raw.record
)

select
    record_id_distinct.record,
    {% for field in fields %}
    pivot_fields.{{ field }}{% if not loop.last %},{% endif %}
    {% endfor %}
from
    record_id_distinct
    left join pivot_fields
        on record_id_distinct.record = pivot_fields.record
