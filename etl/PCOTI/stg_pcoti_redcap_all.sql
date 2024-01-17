with clean_fields as (
    select
        stg_pcoti_redcap_pivot.record,
        stg_pcoti_redcap_pivot.aa_category as accidental_category,
        stg_pcoti_redcap_pivot.account_number::numeric(14, 3) as csn,
        stg_pcoti_redcap_pivot.admit_source,
        coalesce(
            stg_pcoti_redcap_pivot.airway_support_bag_valve_artificial_airway_ind::int8,
            0
        ) as airway_support_bag_valve_artificial_airway_ind,
        coalesce(
            stg_pcoti_redcap_pivot.airway_support_bag_valve_mask_ind::int8,
            0
        ) as airway_support_bag_valve_mask_ind,
        coalesce(
            stg_pcoti_redcap_pivot.airway_support_bipap_ind::int8,
            0
        ) as airway_support_bipap_ind,
        coalesce(
            stg_pcoti_redcap_pivot.airway_support_cpap_ind::int8,
            0
        ) as airway_support_cpap_ind,
        coalesce(
            stg_pcoti_redcap_pivot.airway_support_intubation_ind::int8,
            0
        ) as airway_support_intubation_ind,
        case
            when stg_pcoti_redcap_pivot.attending_saw_cat = 'yes' then 1
            else 0
        end as attending_saw_cat_ind,
        stg_pcoti_redcap_pivot.caller_role2 as caller_role,
        case
            when stg_pcoti_redcap_pivot.cat_calledd = 'yes' then 1
            else 0
        end as cat_called_in_prior_24hrs_ind,
        case
            when stg_pcoti_redcap_pivot.cat_exist_nippv = 'yes' then 1
            else 0
        end as cat_nippv_escalated_ind,
        stg_pcoti_redcap_pivot.cat_fu as cat_follow_up,
        stg_pcoti_redcap_pivot.cat_none_reason,
        stg_pcoti_redcap_pivot.cat_provider as cat_provider_type,
        case
            when stg_pcoti_redcap_pivot.cat_rnfu = 'yes' then 1
            else 0
        end as cat_rn_follow_up_ind,
        case
            when stg_pcoti_redcap_pivot.cc_done = 'yes' then 1
            else 0
        end as cc_done_ind,
        stg_pcoti_redcap_pivot.cc_duration as cc_duration_mins,
        stg_pcoti_redcap_pivot.cc_oneminute as cc_duration_category,
        case
            when stg_pcoti_redcap_pivot.cha_code = 'yes' then 1
            else 0
        end as cha_code_ind,
        stg_pcoti_redcap_pivot.code_category,
        coalesce(
            stg_pcoti_redcap_pivot.code_int_d_stick_ind::int8,
            0
        ) as code_int_d_stick_ind,
        coalesce(
            stg_pcoti_redcap_pivot.code_int_echo_ind::int8,
            0
        ) as code_int_echo_ind,
        coalesce(
            stg_pcoti_redcap_pivot.code_int_labs_ind::int8,
            0
        ) as code_int_labs_ind,
        coalesce(
            stg_pcoti_redcap_pivot.code_int_needle_decompr_ind::int8,
            0
        ) as code_int_needle_decompr_ind,
        coalesce(
            stg_pcoti_redcap_pivot.code_int_other_ind::int8,
            0
        ) as code_int_other_ind,
        coalesce(
            stg_pcoti_redcap_pivot.codenarrator_barrier_computer_access_ind::int8,
            0
        ) as codenarrator_barrier_computer_access_ind,
        coalesce(
            stg_pcoti_redcap_pivot.codenarrator_barrier_epic_issue_ind::int8,
            0
        ) as codenarrator_barrier_epic_issue_ind,
        coalesce(
            stg_pcoti_redcap_pivot.codenarrator_barrier_not_impl_in_unit_ind::int8,
            0
        ) as codenarrator_barrier_not_impl_in_unit_ind,
        coalesce(
            stg_pcoti_redcap_pivot.codenarrator_barrier_other_ind::int8,
            0
        ) as codenarrator_barrier_other_ind,
        coalesce(
            stg_pcoti_redcap_pivot.codenarrator_barrier_staff_comfort_ind::int8,
            0
        ) as codenarrator_barrier_staff_comfort_ind,
        coalesce(
            stg_pcoti_redcap_pivot.codenarrator_barrier_tech_issue_ind::int8,
            0
        ) as codenarrator_barrier_tech_issue_ind,
        case
            when stg_pcoti_redcap_pivot.codenarrator_use = 'yes' then 1
            else 0
        end as code_narrator_ind,
        stg_pcoti_redcap_pivot.cpa_arc as code_cpa_arc,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_als_protocol_ind::int8,
            0
        ) as cqi_cat_als_protocol_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_ascom_ind::int8,
            0
        ) as cqi_cat_ascom_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_cat_recommendations_not_followed_ind::int8,
            0
        ) as cqi_cat_cat_recommendations_not_followed_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_ecg_rhythm_analysis_ind::int8,
            0
        ) as cqi_cat_ecg_rhythm_analysis_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_equipment_ind::int8,
            0
        ) as cqi_cat_equipment_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_family_support_ind::int8,
            0
        ) as cqi_cat_family_support_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_leadership_ind::int8,
            0
        ) as cqi_cat_leadership_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_md_unaware_ind::int8,
            0
        ) as cqi_cat_md_unaware_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_medications_ind::int8,
            0
        ) as cqi_cat_medications_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_notification_ind::int8,
            0
        ) as cqi_cat_notification_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_rn_unaware_ind::int8,
            0
        ) as cqi_cat_rn_unaware_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_roles_ind::int8,
            0
        ) as cqi_cat_roles_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_universal_precautions_ind::int8,
            0
        ) as cqi_cat_universal_precautions_ind,
        coalesce(
            stg_pcoti_redcap_pivot.cqi_cat_unplanned_extubation_ind::int8,
            0
        ) as cqi_cat_unplanned_extubation_ind,
        case
            when stg_pcoti_redcap_pivot.cqi_form = 'yes' then 1
            else 0
        end as cqi_form_ind,
        case
            when stg_pcoti_redcap_pivot.cqi_issues = 'yes' then 1
            else 0
        end as cqi_issues_ind,
        case
            when stg_pcoti_redcap_pivot.dcp = 'yes' then 1
            else 0
        end as dcp_ind,
        case
            when stg_pcoti_redcap_pivot.debrief_cat = 'yes' then 1
            else 0
        end as cat_grp_debrief_ind,
        case
            when stg_pcoti_redcap_pivot.debriefed = 'yes' then 1
            else 0
        end as code_grp_debrief_ind,
        stg_pcoti_redcap_pivot.disposition as immediate_disposition,
        stg_pcoti_redcap_pivot.dnr_event as dnr_status_at_event,
        case
            when stg_pcoti_redcap_pivot.dob is not null and length(stg_pcoti_redcap_pivot.dob) > 1
            then to_date(stg_pcoti_redcap_pivot.dob, 'YYYY-MM-DD')
            else null
        end as dob,
        case
            when stg_pcoti_redcap_pivot.documentation_probs = 'yes' then 1
            else 0
        end as documentation_issues_ind,
        stg_pcoti_redcap_pivot.dx as dx_at_event,
        case
            when stg_pcoti_redcap_pivot.edu_remediation_complete = 'yes' then 1
            else 0
        end as edu_remediation_complete_ind,
        stg_pcoti_redcap_pivot.escalated_to_code,
        to_timestamp(stg_pcoti_redcap_pivot.event_dt_tm, 'YYYY-MM-DD HH24:MI') as event_dt_tm,
        stg_pcoti_redcap_pivot.event_location_details,
        stg_pcoti_redcap_pivot.event_location,
        stg_pcoti_redcap_pivot.event_type,
        stg_pcoti_redcap_pivot.first_name,
        case
            when stg_pcoti_redcap_pivot.followup_pend = 'yes' then 1
            else 0
        end as follow_up_pending_ind,
        to_timestamp(
            stg_pcoti_redcap_pivot.icu_cpa_dt_tm,
            'YYYY-MM-DD HH24:MI'
        ) as icu_cpa_dt_tm,
        case
            when stg_pcoti_redcap_pivot.icu_cpa = 'yes' then 1
            else 0
        end as icu_12hrs_cpa_ind,
        case
            when stg_pcoti_redcap_pivot.keypoint_summary = 'yes' then 1
            else 0
        end as keypoint_summary_complete_ind,
        stg_pcoti_redcap_pivot.last_name,
        case
            when stg_pcoti_redcap_pivot.meds_blood = 'yes' then 1
            else 0
        end as meds_blood,
        coalesce(
            stg_pcoti_redcap_pivot.meds_anti_convulsants_ind::int8,
            0
        ) as meds_anti_convulsants_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_atropine_iv_ind::int8,
            0
        ) as meds_atropine_iv_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_blood_products_ind::int8,
            0
        ) as meds_blood_products_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_epinephrine_im_ind::int8,
            0
        ) as meds_epinephrine_im_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_epinephrine_iv_ind::int8,
            0
        ) as meds_epinephrine_iv_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_fluid_bolus_ind::int8,
            0
        ) as meds_fluid_bolus_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_nmb_vecuronium_ind::int8,
            0
        ) as meds_nmb_vecuronium_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_other_ind::int8,
            0
        ) as meds_other_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_sedative_narcotic_ind::int8,
            0
        ) as meds_sedative_narcotic_ind,
        coalesce(
            stg_pcoti_redcap_pivot.meds_sodium_bicarbonate_iv_ind::int8,
            0
        ) as meds_sodium_bicarbonate_iv_ind,
        stg_pcoti_redcap_pivot.mrn,
        coalesce(
            stg_pcoti_redcap_pivot.neuro_interventions_alt_mental_status_ind::int8,
            0
        ) as neuro_interventions_alt_mental_status_ind,
        coalesce(
            stg_pcoti_redcap_pivot.neuro_interventions_concern_icp_shunt_malfunc_ind::int8,
            0
        ) as neuro_interventions_concern_icp_shunt_malfunc_ind,
        coalesce(
            stg_pcoti_redcap_pivot.neuro_interventions_increase_seizure_freq_ind::int8,
            0
        ) as neuro_interventions_increase_seizure_freq_ind,
        coalesce(
            stg_pcoti_redcap_pivot.neuro_interventions_other_ind::int8,
            0
        ) as neuro_interventions_other_ind,
        stg_pcoti_redcap_pivot.non_patient_category,
        case
            when stg_pcoti_redcap_pivot.nonicuinpt = 'yes' then 1
            else 0
        end as non_icu_inpatient_ind,
        coalesce(
            stg_pcoti_redcap_pivot.nonptescalation::int8,
            0
        ) as non_pt_escalation_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_cardiovascular_change_ind::int8,
            0
        ) as reason_cardiovascular_change_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_family_concern_ind::int8,
            0
        ) as reason_family_concern_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_mpews_ind::int8,
            0
        ) as reason_mpews_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_neurologic_change_ind::int8,
            0
        ) as reason_neurologic_change_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_other_ind::int8,
            0
        ) as reason_other_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_respiratory_change_ind::int8,
            0
        ) as reason_respiratory_change_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_staff_concern_gut_feeling_ind::int8,
            0
        ) as reason_staff_concern_gut_feeling_ind,
        coalesce(
            stg_pcoti_redcap_pivot.reason_vascular_access_ind::int8,
            0
        ) as reason_vascular_access_ind,
        case
            when stg_pcoti_redcap_pivot.sig_complete_leader = 'yes' then 1
            else 0
        end as code_leader_signature_complete_ind,
        case
            when stg_pcoti_redcap_pivot.sig_complete = 'yes' then 1
            else 0
        end as signature_complete_ind,
        stg_pcoti_redcap_pivot.survival_status,
        stg_pcoti_redcap_pivot.telecat_time as telecat_length,
        case
            when stg_pcoti_redcap_pivot.telecat_use = 'yes' then 1
            else 0
        end as telecat_use_ind,
        stg_pcoti_redcap_pivot.timecatleft::time as cat_left_time,
        stg_pcoti_redcap_pivot.timecatreturn::time as cat_return_time,
        stg_pcoti_redcap_pivot.timecbtret2_7a82_8382_8b1::time as code_return_to_picu_time,
        stg_pcoti_redcap_pivot.timefuleave::time as follow_up_leave_time,
        stg_pcoti_redcap_pivot.timefureturn::time as follow_up_return_time,
        stg_pcoti_redcap_pivot.timenprtret2_7a82_838::time as np_return_to_ed_time,
        case
            when stg_pcoti_redcap_pivot.video = 'yes' then 1
            else 0
        end as video_review_performed_ind
    from {{ ref('stg_pcoti_redcap_pivot') }} as stg_pcoti_redcap_pivot
)

select
    coalesce(stg_encounter.pat_key, stg_patient.pat_key) as pat_key,
    stg_encounter.visit_key,
    clean_fields.*
from
    clean_fields
    left join {{ ref('stg_patient') }} as stg_patient
        on clean_fields.mrn = stg_patient.mrn
    left join {{ ref('stg_encounter') }} as stg_encounter
        on clean_fields.csn = stg_encounter.csn
