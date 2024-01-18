with redcap_raw as (
    select
        'NEAR4KIDS' as registry,
        near4kids_encounter.encounterdate as encounter_date,
        near4kids_encounter.encountertime as encounter_time,
        near4kids_encounter.encounter_id,
        case
            when lower(near4kids_encounter.centername) like '%philadelphia%koph%' then 'CHOP - KOPH'
            when lower(near4kids_encounter.centername) like '%philadelphia%' then 'CHOP'
        end as center_name,
        case
            when near4kids_encounter.encounter_id like '11566-%' then 'PHL'
            when near4kids_encounter.encounter_id like '21881-%' then 'KOPH'
        end as enrollment_center,
        near4kids_encounter.location_of_intubation,
        near4kids_encounter.indicationtype as indication_type,
        near4kids_course.courseid as course_id,
        near4kids_course.course_number,
        near4kids_course.numofattempts as num_of_attempts,
        near4kids_course.coursesuccess,
        near4kids_course.a1_success,
        near4kids_course.tiaes_course,
        near4kids_encounter.historyofdifficultairway,
        near4kids_course.tiae___1 as tiae_cardiac_arrest_died,
        near4kids_course.tiae___2 as tiae_cardiac_arrest_survived,
        near4kids_course.tiae___3 as tiae_mainstem_intubation,
        near4kids_course.tiae___4 as tiae_esophageal_intubation_immediate_recognition,
        near4kids_course.tiae___5 as tiae_esophageal_intubation_delayed_recognition,
        near4kids_course.tiae___6 as tiae_vomit_with_aspiration,
        near4kids_course.tiae___7 as tiae_vomit_no_aspiration,
        near4kids_course.tiae___8 as tiae_hypotension_needs_intervention,
        near4kids_course.tiae___9 as tiae_hypertension_requiring_therapy,
        null as tiae_cardiac_compressions_under_1min,
        near4kids_course.tiae___10 as tiae_epistaxis,
        near4kids_course.tiae___11 as tiae_gum_dental_trauma,
        near4kids_course.tiae___12 as tiae_lip_trauma,
        near4kids_course.tiae___13 as tiae_laryngospasm,
        near4kids_course.tiae___14 as tiae_malignant_hyperthermia,
        near4kids_course.tiae___15 as tiae_medication_error,
        near4kids_course.tiae___16 as tiae_pneumothorax_pneumomediastinum,
        near4kids_course.tiae___17 as tiae_direct_airway_injury,
        near4kids_course.tiae___18 as tiae_dysrhythmia,
        near4kids_course.tiae___19 as tiae_pain_agitation,
        near4kids_course.tiae___21 as tiae_other,
        near4kids_course.pulselowest,
        near4kids_course.pulsestart,
        null as lowest_heart_rate,
        null as highest_heart_rate,
        near4kids_encounter.indications1___1 as indications_oxygen_failure,
        near4kids_encounter.indications1___2 as indications_procedure,
        near4kids_encounter.indications1___3 as indications_ventilation_failure,
        near4kids_encounter.indications1___4 as indications_frequent_apnea_bradycardia,
        near4kids_encounter.indications1___5 as indications_upper_airway_obstruction,
        near4kids_encounter.indications1___6 as indications_therapeutic_hyperventilation,
        near4kids_encounter.indications1___16 as indications_airway_clearance,
        near4kids_encounter.indications1___7 as indications_neuromuscular_weakness,
        near4kids_encounter.indications1___8 as indications_emergency_drug_admin,
        near4kids_encounter.indications1___9 as indications_unstable_hemodynamics,
        near4kids_encounter.indications1___10 as indications_absent_protective_airway_reflexes,
        null as indications_surfactant_administration,
        null as indications_delivery_room_routine,
        null as indications_delivery_room_clinical,
        near4kids_encounter.indications1___14 as indications_reintubation_after_ue,
        null as indications_stability_for_transport,
        near4kids_encounter.indications1___15 as indications_other,
        near4kids_encounter.patientagedays as patient_age_days,
        near4kids_course.aou,
        near4kids_course.device,
        near4kids_course.video_laryngo_type,
        near4kids_encounter.encounterchecklistused as encounter_checklist_used,
        near4kids_course.rocuronium,
        near4kids_course.succinylcholine,
        near4kids_course.vecuronium,
        near4kids_course.pancuronium,
        near4kids_course.cisatracuronium,
        near4kids_course.atropine,
        null as comorbidities_sepsis,
        null as comorbidities_congenital_cardiac_disease,
        null as comorbidities_anatomic_congenital_anomaly,
        null as comorbidities_airway_craniofacial_anomaly_req_surgery,
        null as comorbidities_neurological_impairment,
        null as comorbidities_actute_respiratory_failure,
        null as comorbidities_chronic_respiratory_failure,
        null as comorbidities_surgery_procedure_for_acquired_procedure
    from
        {{source('ods', 'near4kids_encounter')}} as near4kids_encounter
        inner join {{source('ods', 'near4kids_course')}} as near4kids_course
            on near4kids_encounter.encounter_id = coalesce(near4kids_course.encounter_20630_sql,
                near4kids_course.encounter_id_verify)

    union all

    select
        'NEAR4TRANSPORT' as registry,
        near4transport_encounter.encounterdate as encounter_date,
        near4transport_encounter.encountertime as encounter_time,
        near4transport_encounter.encounter_id,
        case
            when lower(near4transport_encounter.centername) like '%philadelphia%koph%'
                then 'CHOP - KOPH'
            when lower(near4transport_encounter.centername) like '%philadelphia%'
                then 'CHOP'
        end as center_name,
        case
            when near4transport_encounter.encounter_id like '13367-%' then 'PHL'
        end as enrollment_center,
        near4transport_encounter.location_of_intubation,
        near4transport_encounter.indicationtype as indication_type,
        near4transport_course.courseid as course_id,
        near4transport_course.course_number,
        near4transport_course.numofattempts as num_of_attempts,
        near4transport_course.coursesuccess,
        near4transport_course.a1_success,
        near4transport_course.tiaes_course,
        near4transport_encounter.historyofdifficultairway,
        near4transport_course.tiae___1 as tiae_cardiac_arrest_died,
        near4transport_course.tiae___2 as tiae_cardiac_arrest_survived,
        near4transport_course.tiae___3 as tiae_mainstem_intubation,
        near4transport_course.tiae___4 as tiae_esophageal_intubation_immediate_recognition,
        near4transport_course.tiae___5 as tiae_esophageal_intubation_delayed_recognition,
        near4transport_course.tiae___6 as tiae_vomit_with_aspiration,
        near4transport_course.tiae___7 as tiae_vomit_no_aspiration,
        near4transport_course.tiae___8 as tiae_hypotension_needs_intervention,
        near4transport_course.tiae___9 as tiae_hypertension_requiring_therapy,
        null as tiae_cardiac_compressions_under_1min,
        near4transport_course.tiae___10 as tiae_epistaxis,
        near4transport_course.tiae___11 as tiae_gum_dental_trauma,
        near4transport_course.tiae___12 as tiae_lip_trauma,
        near4transport_course.tiae___13 as tiae_laryngospasm,
        near4transport_course.tiae___14 as tiae_malignant_hyperthermia,
        near4transport_course.tiae___15 as tiae_medication_error,
        near4transport_course.tiae___16 as tiae_pneumothorax_pneumomediastinum,
        near4transport_course.tiae___17 as tiae_direct_airway_injury,
        near4transport_course.tiae___18 as tiae_dysrhythmia,
        near4transport_course.tiae___19 as tiae_pain_agitation,
        near4transport_course.tiae___21 as tiae_other,
        near4transport_course.pulselowest,
        near4transport_course.pulsestart,
        near4transport_course.lowest_heart_rate,
        near4transport_course.highest_heart_rate,
        near4transport_encounter.indications1___1 as indications_oxygen_failure,
        null as indications_procedure,
        near4transport_encounter.indications1___2 as indications_ventilation_failure,
        near4transport_encounter.indications1___3 as indications_frequent_apnea_bradycardia,
        near4transport_encounter.indications1___4 as indications_upper_airway_obstruction,
        near4transport_encounter.indications1___5 as indications_therapeutic_hyperventilation,
        near4transport_encounter.indications1___6 as indications_airway_clearance,
        near4transport_encounter.indications1___7 as indications_neuromuscular_weakness,
        null as indications_emergency_drug_admin,
        near4transport_encounter.indications1___8 as indications_unstable_hemodynamics,
        near4transport_encounter.indications1___9 as indications_absent_protective_airway_reflexes,
        near4transport_encounter.indications1___10 as indications_surfactant_administration,
        null as indications_delivery_room_routine,
        null as indications_delivery_room_clinical,
        near4transport_encounter.indications1___11 as indications_reintubation_after_ue,
        near4transport_encounter.indications1___12 as indications_stability_for_transport,
        near4transport_encounter.indications1___13 as indications_other,
        near4transport_encounter.patientagedays as patient_age_days,
        null as aou,
        near4transport_course.device,
        near4transport_course.video_laryngo_type,
        null as encounter_checklist_used,
        near4transport_course.rocuronium,
        near4transport_course.succinylcholine,
        near4transport_course.vecuronium,
        null as pancuronium,
        null as cisatracuronium,
        near4transport_course.atropine,
        null as comorbidities_sepsis,
        null as comorbidities_congenital_cardiac_disease,
        null as comorbidities_anatomic_congenital_anomaly,
        null as comorbidities_airway_craniofacial_anomaly_req_surgery,
        null as comorbidities_neurological_impairment,
        null as comorbidities_actute_respiratory_failure,
        null as comorbidities_chronic_respiratory_failure,
        null as comorbidities_surgery_procedure_for_acquired_procedure
    from
        {{source('ods_redcap_research', 'near4transport_encounter')}} as near4transport_encounter
        inner join {{source('ods_redcap_research', 'near4transport_course')}} as near4transport_course
            on near4transport_encounter.encounter_id = coalesce(near4transport_course.encounter_23886_sql,
                near4transport_course.encounter_id_verify)

    union all

    select
        'NEAR4NEOS' as registry,
        near4neos_encounter.encounter_date as encounter_date,
        near4neos_encounter.encounter_time as encounter_time,
        near4neos_encounter.encounter_number as encounter_id,
        near4neos_encounter.site_of_intubation as center_name,
        case
            when near4neos_encounter.encounter_number like '5507-%' then 'PHL'
        end as enrollment_center,
        near4neos_encounter.location_of_intubation,
        near4neos_encounter.type_intubation as indication_type,
        near4neos_course.course_id,
        near4neos_course.course_number,
        near4neos_course.attempts as num_of_attempts,
        near4neos_course.success as coursesuccess,
        near4neos_course.a1_success,
        near4neos_course.tiaes as tiaes_course,
        near4neos_encounter.known_hx_diff_airway as historyofdifficultairway,
        near4neos_course.tiae___1 as tiae_cardiac_arrest_died,
        near4neos_course.tiae___2 as tiae_cardiac_arrest_survived,
        near4neos_course.tiae___3 as tiae_mainstem_intubation,
        near4neos_course.tiae___4 as tiae_esophageal_intubation_immediate_recognition,
        near4neos_course.tiae___5 as tiae_esophageal_intubation_delayed_recognition,
        near4neos_course.tiae___6 as tiae_vomit_with_aspiration,
        near4neos_course.tiae___7 as tiae_vomit_no_aspiration,
        near4neos_course.tiae___8 as tiae_hypotension_needs_intervention,
        near4neos_course.tiae___9 as tiae_hypertension_requiring_therapy,
        near4neos_course.tiae___10 as tiae_cardiac_compressions_under_1min,
        near4neos_course.tiae___11 as tiae_epistaxis,
        near4neos_course.tiae___12 as tiae_gum_dental_trauma,
        near4neos_course.tiae___13 as tiae_lip_trauma,
        near4neos_course.tiae___14 as tiae_laryngospasm,
        near4neos_course.tiae___15 as tiae_malignant_hyperthermia,
        near4neos_course.tiae___16 as tiae_medication_error,
        near4neos_course.tiae___17 as tiae_pneumothorax_pneumomediastinum,
        near4neos_course.tiae___18 as tiae_direct_airway_injury,
        near4neos_course.tiae___19 as tiae_dysrhythmia,
        near4neos_course.tiae___20 as tiae_pain_agitation,
        near4neos_course.tiae___21 as tiae_other,
        near4neos_course.lowest_pulse_oximetry as pulselowest,
        near4neos_course.highest_pulse_oximetry as pulsestart,
        near4neos_course.lowest_heart_rate,
        near4neos_course.highest_heart_rate,
        near4neos_encounter.initial_intubation_indicat___1 as indications_oxygen_failure,
        near4neos_encounter.initial_intubation_indicat___2 as indications_procedure,
        near4neos_encounter.initial_intubation_indicat___3 as indications_ventilation_failure,
        near4neos_encounter.initial_intubation_indicat___4 as indications_frequent_apnea_bradycardia,
        near4neos_encounter.initial_intubation_indicat___5 as indications_upper_airway_obstruction,
        near4neos_encounter.initial_intubation_indicat___6 as indications_therapeutic_hyperventilation,
        near4neos_encounter.initial_intubation_indicat___16 as indications_airway_clearance,
        near4neos_encounter.initial_intubation_indicat___7 as indications_neuromuscular_weakness,
        near4neos_encounter.initial_intubation_indicat___8 as indications_emergency_drug_admin,
        near4neos_encounter.initial_intubation_indicat___9 as indications_unstable_hemodynamics,
        near4neos_encounter.initial_intubation_indicat___10 as indications_absent_protective_airway_reflexes,
        near4neos_encounter.initial_intubation_indicat___11 as indications_surfactant_administration,
        near4neos_encounter.initial_intubation_indicat___12 as indications_delivery_room_routine,
        near4neos_encounter.initial_intubation_indicat___13 as indications_delivery_room_clinical,
        near4neos_encounter.initial_intubation_indicat___14 as indications_reintubation_after_ue,
        null as indications_stability_for_transport,
        near4neos_encounter.initial_intubation_indicat___15 as indications_other,
        near4neos_encounter.age_in_days as patient_age_days,
        null as aou,
        near4neos_course.device,
        near4neos_course.c1_video_laryngo_type as video_laryngo_type,
        near4neos_encounter.airway_bundle_status as encounter_checklist_used,
        near4neos_course.rocuronium,
        near4neos_course.succinylcholine,
        near4neos_course.vecuronium,
        near4neos_course.pancuronium,
        near4neos_course.cisatracuronium,
        near4neos_course.atropine,
        near4neos_encounter.categories_comorbidities___9 as comorbidities_sepsis,
        near4neos_encounter.categories_comorbidities___10 as comorbidities_congenital_cardiac_disease,
        near4neos_encounter.categories_comorbidities___11 as comorbidities_anatomic_congenital_anomaly,
        near4neos_encounter.categories_comorbidities___12 as comorbidities_airway_craniofacial_anomaly_req_surgery,
        near4neos_encounter.categories_comorbidities___13 as comorbidities_neurological_impairment,
        near4neos_encounter.categories_comorbidities___14 as comorbidities_actute_respiratory_failure,
        near4neos_encounter.categories_comorbidities___15 as comorbidities_chronic_respiratory_failure,
        near4neos_encounter.categories_comorbidities___16 as comorbidities_surgery_procedure_for_acquired_procedure
    from
        {{source('ods_redcap_research', 'near4neos_encounter')}} as near4neos_encounter
        inner join {{source('ods_redcap_research', 'near4neos_course')}} as near4neos_course
            on near4neos_encounter.encounter_number = coalesce(near4neos_course.encounter_9751_sql,
            near4neos_course.encounter_number)
)

select
    redcap_raw.registry,
    date(to_char(redcap_raw.encounter_date, 'yyyy-mm-01')) as encounter_month,
    redcap_raw.encounter_date,
    redcap_raw.encounter_time,
    (to_char(redcap_raw.encounter_date, 'YYYY-MM-DD') || ' ' || redcap_raw.encounter_time)::datetime
    as encounter_datetime,
    redcap_raw.encounter_id,
    redcap_raw.center_name,
    redcap_raw.enrollment_center,
    redcap_raw.location_of_intubation,
    redcap_raw.indication_type,
    redcap_raw.course_id,
    redcap_raw.course_number,
    redcap_raw.num_of_attempts,
    case
        when redcap_raw.coursesuccess = 'Yes' then 1
        when redcap_raw.coursesuccess = 'No' then 0
    end as course_success_ind,
    case
        when redcap_raw.a1_success = 'Yes' then 1
        when redcap_raw.a1_success = 'No' then 0
    end as a1_success_ind,
    case
        when redcap_raw.tiaes_course = 'Yes' then 1
        when redcap_raw.tiaes_course = 'No' then 0
    end as tiaes_course_ind,
    case
        when redcap_raw.historyofdifficultairway = 'Yes' then 1
        when redcap_raw.historyofdifficultairway = 'No' then 0
    end as history_of_difficult_airway_ind,
    case
        when redcap_raw.tiae_cardiac_arrest_died = 'Checked' then 1
        when redcap_raw.tiae_cardiac_arrest_died = 'Unchecked' then 0
    end as tiae_cardiac_arrest_died_ind,
    case
        when redcap_raw.tiae_cardiac_arrest_survived = 'Checked' then 1
        when redcap_raw.tiae_cardiac_arrest_survived = 'Unchecked' then 0
    end as tiae_cardiac_arrest_survived_ind,
    case
        when redcap_raw.tiae_mainstem_intubation = 'Checked' then 1
        when redcap_raw.tiae_mainstem_intubation = 'Unchecked' then 0
    end as tiae_mainstem_intubation_ind,
    case
        when redcap_raw.tiae_esophageal_intubation_immediate_recognition = 'Checked' then 1
        when redcap_raw.tiae_esophageal_intubation_immediate_recognition = 'Unchecked' then 0
    end as tiae_esophageal_intubation_immediate_recognition_ind,
    case
        when redcap_raw.tiae_esophageal_intubation_delayed_recognition = 'Checked' then 1
        when redcap_raw.tiae_esophageal_intubation_delayed_recognition = 'Unchecked' then 0
    end as tiae_esophageal_intubation_delayed_recognition_ind,
    case
        when redcap_raw.tiae_vomit_with_aspiration = 'Checked' then 1
        when redcap_raw.tiae_vomit_with_aspiration = 'Unchecked' then 0
    end as tiae_vomit_with_aspiration_ind,
    case
        when redcap_raw.tiae_vomit_no_aspiration = 'Checked' then 1
        when redcap_raw.tiae_vomit_no_aspiration = 'Unchecked' then 0
    end as tiae_vomit_no_aspiration_ind,
    case
        when redcap_raw.tiae_hypotension_needs_intervention = 'Checked' then 1
        when redcap_raw.tiae_hypotension_needs_intervention = 'Unchecked' then 0
    end as tiae_hypotension_needs_intervention_ind,
    case
        when redcap_raw.tiae_hypertension_requiring_therapy = 'Checked' then 1
        when redcap_raw.tiae_hypertension_requiring_therapy = 'Unchecked' then 0
    end as tiae_hypertension_requiring_therapy_ind,
    case
        when redcap_raw.tiae_cardiac_compressions_under_1min = 'Checked' then 1
        when redcap_raw.tiae_cardiac_compressions_under_1min = 'Unchecked' then 0
    end as tiae_cardiac_compressions_under_1min_ind,
    case
        when redcap_raw.tiae_epistaxis = 'Checked' then 1
        when redcap_raw.tiae_epistaxis = 'Unchecked' then 0
    end as tiae_epistaxis_ind,
    case
        when redcap_raw.tiae_gum_dental_trauma = 'Checked' then 1
        when redcap_raw.tiae_gum_dental_trauma = 'Unchecked' then 0
    end as tiae_gum_dental_trauma_ind,
    case
        when redcap_raw.tiae_lip_trauma = 'Checked' then 1
        when redcap_raw.tiae_lip_trauma = 'Unchecked' then 0
    end as tiae_lip_trauma_ind,
    case
        when redcap_raw.tiae_laryngospasm = 'Checked' then 1
        when redcap_raw.tiae_laryngospasm = 'Unchecked' then 0
    end as tiae_laryngospasm_ind,
    case
        when redcap_raw.tiae_malignant_hyperthermia = 'Checked' then 1
        when redcap_raw.tiae_malignant_hyperthermia = 'Unchecked' then 0
    end as tiae_malignant_hyperthermia_ind,
    case
        when redcap_raw.tiae_medication_error = 'Checked' then 1
        when redcap_raw.tiae_medication_error = 'Unchecked' then 0
    end as tiae_medication_error_ind,
    case
        when redcap_raw.tiae_pneumothorax_pneumomediastinum = 'Checked' then 1
        when redcap_raw.tiae_pneumothorax_pneumomediastinum = 'Unchecked' then 0
    end as tiae_pneumothorax_pneumomediastinum_ind,
    case
        when redcap_raw.tiae_direct_airway_injury = 'Checked' then 1
        when redcap_raw.tiae_direct_airway_injury = 'Unchecked' then 0
    end as tiae_direct_airway_injury_ind,
    case
        when redcap_raw.tiae_dysrhythmia = 'Checked' then 1
        when redcap_raw.tiae_dysrhythmia = 'Unchecked' then 0
    end as tiae_dysrhythmia_ind,
    case
        when redcap_raw.tiae_pain_agitation = 'Checked' then 1
        when redcap_raw.tiae_pain_agitation = 'Unchecked' then 0
    end as tiae_pain_agitation_ind,
    case
        when redcap_raw.tiae_other = 'Checked' then 1
        when redcap_raw.tiae_other = 'Unchecked' then 0
    end as tiae_other_ind,
    redcap_raw.pulselowest as pulse_lowest,
    redcap_raw.pulsestart as pulse_start,
    redcap_raw.lowest_heart_rate,
    redcap_raw.highest_heart_rate,
    case
        when redcap_raw.indications_oxygen_failure = 'Checked' then 1
        when redcap_raw.indications_oxygen_failure = 'Unchecked' then 0
    end as indications_oxygen_failure_ind,
    case
        when redcap_raw.indications_procedure = 'Checked' then 1
        when redcap_raw.indications_procedure = 'Unchecked' then 0
    end as indications_procedure_ind,
    case
        when redcap_raw.indications_ventilation_failure = 'Checked' then 1
        when redcap_raw.indications_ventilation_failure = 'Unchecked' then 0
    end as indications_ventilation_failure_ind,
    case
        when redcap_raw.indications_frequent_apnea_bradycardia = 'Checked' then 1
        when redcap_raw.indications_frequent_apnea_bradycardia = 'Unchecked' then 0
    end as indications_frequent_apnea_bradycardia_ind,
    case
        when redcap_raw.indications_upper_airway_obstruction = 'Checked' then 1
        when redcap_raw.indications_upper_airway_obstruction = 'Unchecked' then 0
    end as indications_upper_airway_obstruction_ind,
    case
        when redcap_raw.indications_therapeutic_hyperventilation = 'Checked' then 1
        when redcap_raw.indications_therapeutic_hyperventilation = 'Unchecked' then 0
    end as indications_therapeutic_hyperventilation_ind,
    case
        when redcap_raw.indications_airway_clearance = 'Checked' then 1
        when redcap_raw.indications_airway_clearance = 'Unchecked' then 0
    end as indications_airway_clearance_ind,
    case
        when redcap_raw.indications_neuromuscular_weakness = 'Checked' then 1
        when redcap_raw.indications_neuromuscular_weakness = 'Unchecked' then 0
    end as indications_neuromuscular_weakness_ind,
    case
        when redcap_raw.indications_emergency_drug_admin = 'Checked' then 1
        when redcap_raw.indications_emergency_drug_admin = 'Unchecked' then 0
    end as indications_emergency_drug_admin_ind,
    case
        when redcap_raw.indications_unstable_hemodynamics = 'Checked' then 1
        when redcap_raw.indications_unstable_hemodynamics = 'Unchecked' then 0
    end as indications_unstable_hemodynamics_ind,
    case
        when redcap_raw.indications_absent_protective_airway_reflexes = 'Checked' then 1
        when redcap_raw.indications_absent_protective_airway_reflexes = 'Unchecked' then 0
    end as indications_absent_protective_airway_reflexes_ind,
    case
        when redcap_raw.indications_surfactant_administration = 'Checked' then 1
        when redcap_raw.indications_surfactant_administration = 'Unchecked' then 0
    end as indications_surfactant_administration_ind,
    case
        when redcap_raw.indications_delivery_room_routine = 'Checked' then 1
        when redcap_raw.indications_delivery_room_routine = 'Unchecked' then 0
    end as indications_delivery_room_routine_ind,
    case
        when redcap_raw.indications_delivery_room_clinical = 'Checked' then 1
        when redcap_raw.indications_delivery_room_clinical = 'Unchecked' then 0
    end as indications_delivery_room_clinical_ind,
    case
        when redcap_raw.indications_reintubation_after_ue = 'Checked' then 1
        when redcap_raw.indications_reintubation_after_ue = 'Unchecked' then 0
    end as indications_reintubation_after_ue_ind,
    case
        when redcap_raw.indications_stability_for_transport = 'Checked' then 1
        when redcap_raw.indications_stability_for_transport = 'Unchecked' then 0
    end as indications_stability_for_transport_ind,
    case
        when redcap_raw.indications_other = 'Checked' then 1
        when redcap_raw.indications_other = 'Unchecked' then 0
    end as indications_other_ind,
    redcap_raw.patient_age_days,
    case
        when redcap_raw.aou = 'Yes' then 1
        when redcap_raw.aou = 'No' then 0
    end as aou_ind,
    redcap_raw.device,
    redcap_raw.video_laryngo_type,
    redcap_raw.encounter_checklist_used,
    redcap_raw.rocuronium,
    redcap_raw.succinylcholine,
    redcap_raw.vecuronium,
    redcap_raw.pancuronium,
    redcap_raw.cisatracuronium,
    redcap_raw.atropine,
    case
        when redcap_raw.comorbidities_sepsis = 'Checked' then 1
        when redcap_raw.comorbidities_sepsis = 'Unchecked' then 0
    end as comorbidities_sepsis_ind,
    case
        when redcap_raw.comorbidities_congenital_cardiac_disease = 'Checked' then 1
        when redcap_raw.comorbidities_congenital_cardiac_disease = 'Unchecked' then 0
    end as comorbidities_congenital_cardiac_disease_ind,
    case
        when redcap_raw.comorbidities_anatomic_congenital_anomaly = 'Checked' then 1
        when redcap_raw.comorbidities_anatomic_congenital_anomaly = 'Unchecked' then 0
    end as comorbidities_anatomic_congenital_anomaly_ind,
    case
        when redcap_raw.comorbidities_neurological_impairment = 'Checked' then 1
        when redcap_raw.comorbidities_neurological_impairment = 'Unchecked' then 0
    end as comorbidities_neurological_impairment_ind,
    case
        when redcap_raw.comorbidities_actute_respiratory_failure = 'Checked' then 1
        when redcap_raw.comorbidities_actute_respiratory_failure = 'Unchecked' then 0
    end as comorbidities_actute_respiratory_failure_ind,
    case
        when redcap_raw.comorbidities_chronic_respiratory_failure = 'Checked' then 1
        when redcap_raw.comorbidities_chronic_respiratory_failure = 'Unchecked' then 0
    end as comorbidities_chronic_respiratory_failure_ind,
    case
        when redcap_raw.comorbidities_surgery_procedure_for_acquired_procedure = 'Checked' then 1
        when redcap_raw.comorbidities_surgery_procedure_for_acquired_procedure = 'Unchecked' then 0
    end as comorbidities_surgery_procedure_for_acquired_procedure_ind
from
    redcap_raw
