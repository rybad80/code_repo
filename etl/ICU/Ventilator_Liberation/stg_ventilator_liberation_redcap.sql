with encounter_information as (
    select
        local_encounter_id,
        multicenter_encounter,
        name,
        mrn,
        csn,
        dob,
        type_icu,
        date_time_icu_admission,
        age_days,
        sex,
        race_ethinicity___9,
        race_ethinicity___1,
        race_ethinicity___2,
        race_ethinicity___10,
        race_ethinicity___3,
        race_ethinicity___11,
        race_ethinicity___4,
        race_ethinicity___12,
        race_ethinicity___5,
        race_ethinicity___7,
        race_ethinicity___6,
        race_ethinicity___8,
        race_other,
        pim3,
        pmh_nmdz,
        pmh_nmdz_type___1,
        pmh_nmdz_type___2,
        pmh_nmdz_type___3,
        pmh_nmdz_type___4,
        pmh_nmdz_type___5,
        pmh_nmdz_type___6,
        pmh_nmdz_type___7,
        other_nmdz,
        pmh_cogimp,
        pmh_cogimp_pcpc,
        pmh_cyanhd,
        pmh_cyanhd_type___1,
        pmh_cyanhd_type___2,
        pmh_cyanhd_type___3,
        pmh_cyanhd_type___4,
        pmh_cyanhd_type___5,
        pmh_cyanhd_type___6,
        pmh_cyanhd_type___7,
        other_cyanhd,
        pmh_ncyanhd,
        pmh_ncyanhd_type___1,
        pmh_ncyanhd_type___2,
        pmh_ncyanhd_type___3,
        pmh_ncyanhd_type___4,
        pmh_ncyanhd_type___5,
        pmh_ncyanhd_type___6,
        pmh_ncyanhd_type___7,
        pmh_ncyanhd_type___8,
        other_ncyanhd,
        pmh_pulmhtn,
        pmh_premature,
        pmh_premat_type,
        pmh_parlungdz,
        pmh_parlungdz_type___1,
        pmh_parlungdz_type___2,
        pmh_parlungdz_type___3,
        pmh_parlungdz_type___4,
        pmh_parlungdz_type___5,
        pmh_parlungdz_type___6,
        other_parlungdz,
        pmh_lowlungdz,
        pmh_lowlungdz_type___1,
        pmh_lowlungdz_type___2,
        pmh_lowlungdz_type___3,
        pmh_lowlungdz_type___4,
        pmh_lowlungdz_type___5,
        pmh_lowlungdz_type___6,
        pmh_lowlungdz_type___7,
        other_lowlungdz,
        pmh_uadz,
        pmh_uadz_type___1,
        pmh_uadz_type___2,
        pmh_uadz_type___3,
        pmh_uadz_type___4,
        pmh_uadz_type___5,
        other_uadz,
        pmh_respsupp,
        pmh_respsupp_type,
        pmh_oncdz,
        pmh_oncdz_treat,
        pmh_oncdz_type___1,
        pmh_oncdz_type___2,
        pmh_oncdz_type___3,
        pmh_oncdz_type___4,
        pmh_oncdz_type___5,
        pmh_oncdz_type___6,
        pmh_oncdz_type___7,
        pmh_oncdz_type___8,
        other_oncdz,
        pmh_bmt,
        pmh_bmt_type,
        pmh_bmt_days,
        pmh_imm_comp,
        pmh_imm_comp_type___1,
        pmh_imm_comp_type___2,
        pmh_imm_comp_type___3,
        pmh_imm_comp_type___4,
        pmh_imm_comp_type___5,
        pmh_imm_comp_type___6,
        pmh_imm_comp_type___7,
        pmh_imm_comp_type___8,
        pmh_imm_comp_type___9,
        other_imm_comp_type,
        primarydxicuadmit___1,
        primarydxicuadmit___2,
        primarydxicuadmit___3,
        primarydxicuadmit___4,
        primarydxicuadmit___5,
        primarydxicuadmit___6,
        primarydxicuadmit___7,
        primarydxicuadmit___8,
        primarydxicuadmit___9,
        primarydxicuadmit___10,
        primarydxicuadmit___11,
        primarydxicuadmit___12,
        primarydxicuadmit___13,
        other_med_admin,
        other_surg_admin,
        encounter_information_complete
    from
        {{source('ods_redcap_research', 'redcap_ventlib_local')}}
    where
        encounter_information_complete is not null
),

intubation_course as (
    select
        local_encounter_id,
        redcap_repeat_instrument,
        redcap_repeat_instance,
        lda_id,
        intubation_datetime,
        time_admit_ti,
        age_y_intubation,
        n4k_encounter,
        indication_intubation___1,
        indication_intubation___2,
        indication_intubation___3,
        indication_intubation___4,
        indication_intubation___5,
        indication_intubation___6,
        indication_intubation___7,
        indication_intubation___8,
        indication_intubation___9,
        indication_intubation___10,
        indication_intubation___11,
        indication_intubation___99,
        indication_intubation___12,
        other_ti_indication,
        uao_location,
        intubation_weight,
        intubation_height,
        ett_size,
        ett_cuff,
        ett_location,
        cardiacsurg,
        int_attempts,
        any_extub_attempt,
        why_never_ext,
        extubation_weight,
        extubation_height,
        ext_att_date_time as extubation_datetime,
        length_ti,
        ext_loc,
        ext_codestatus,
        ext_type_planned,
        risk_perception,
        diff_airway,
        sed_score_type,
        other_sed_score,
        sbs_score,
        comfortb_score,
        rass_score,
        gcs,
        sbt_done,
        minimalvent_nonsbt,
        pip,
        set_rr,
        set_peep,
        fio2,
        set_ps,
        sbt_support_strategy,
        peep_cpap,
        ps_sbt,
        sbt_fio2,
        sbt_duration,
        sbt_result,
        sbt_fail_reason___1,
        sbt_fail_reason___2,
        sbt_fail_reason___3,
        sbt_fail_reason___4,
        sbt_fail_reason___5,
        sbt_fail_reason___6,
        sbt_fail_reason___7,
        sbt_fail_reason___8,
        sbt_fail_reason___9,
        other_sbt_fail,
        airleak,
        airleak_pressure,
        nifpimax,
        nifpimax_value,
        steroids,
        steroid_type,
        steroid_dose,
        steroid_frequency,
        steroid_timing,
        plan_ext_initial_nrs,
        unplan_ext_initial_nrs,
        unplan_nrs,
        unplan_nrs_type___1,
        unplan_nrs_type___2,
        unplan_nrs_type___3,
        unplan_nrs_type___4,
        hfnc_initial_flow,
        hfnc_max_flow,
        course_exposures___1,
        course_exposures___2,
        course_exposures___3,
        course_exposures___4,
        course_exposures___5,
        course_exposures___6,
        course_exposures___7,
        ext_attempt_outcome,
        ext_fail_causes___1,
        ext_fail_causes___2,
        ext_fail_causes___3,
        ext_fail_causes___4,
        ext_fail_causes___5,
        ext_fail_causes___6,
        ext_fail_causes___7,
        ext_fail_causes___8,
        ext_fail_causes___9,
        ext_fail_causes___10,
        ext_fail_causes___11,
        ext_fail_causes___12,
        ext_fail_causes___13,
        ext_fail_causes___14,
        ext_fail_causes___15,
        other_ext_fail_cause,
        ext_fail_uao_loc,
        qualify_transfer,
        intubation_course_complete
    from
        {{source('ods_redcap_research', 'redcap_ventlib_local')}}
    where
        redcap_repeat_instrument = 'intubation_course'
),

encounter_outcomes as (
    select
        local_encounter_id,
        icu_end_datetime,
        icu_disposition,
        time_extub_dispo,
        icu_los,
        encounter_outcomes_complete
    from
        {{source('ods_redcap_research', 'redcap_ventlib_local')}}
    where
        encounter_outcomes_complete is not null
)

select
    intubation_course.lda_id,
    intubation_course.redcap_repeat_instance::bigint as redcap_repeat_instance,
    intubation_course.redcap_repeat_instrument,
    encounter_information.local_encounter_id::bigint as local_encounter_id,
    encounter_information.multicenter_encounter,
    encounter_information.name,
    encounter_information.mrn,
    encounter_information.csn,
    encounter_information.dob,
    encounter_information.type_icu,
    encounter_information.date_time_icu_admission,
    encounter_information.age_days,
    encounter_information.sex,
    encounter_information.race_ethinicity___9,
    encounter_information.race_ethinicity___1,
    encounter_information.race_ethinicity___2,
    encounter_information.race_ethinicity___10,
    encounter_information.race_ethinicity___3,
    encounter_information.race_ethinicity___11,
    encounter_information.race_ethinicity___4,
    encounter_information.race_ethinicity___12,
    encounter_information.race_ethinicity___5,
    encounter_information.race_ethinicity___7,
    encounter_information.race_ethinicity___6,
    encounter_information.race_ethinicity___8,
    encounter_information.race_other,
    encounter_information.pim3,
    encounter_information.pmh_nmdz,
    encounter_information.pmh_nmdz_type___1,
    encounter_information.pmh_nmdz_type___2,
    encounter_information.pmh_nmdz_type___3,
    encounter_information.pmh_nmdz_type___4,
    encounter_information.pmh_nmdz_type___5,
    encounter_information.pmh_nmdz_type___6,
    encounter_information.pmh_nmdz_type___7,
    encounter_information.other_nmdz,
    encounter_information.pmh_cogimp,
    encounter_information.pmh_cogimp_pcpc,
    encounter_information.pmh_cyanhd,
    encounter_information.pmh_cyanhd_type___1,
    encounter_information.pmh_cyanhd_type___2,
    encounter_information.pmh_cyanhd_type___3,
    encounter_information.pmh_cyanhd_type___4,
    encounter_information.pmh_cyanhd_type___5,
    encounter_information.pmh_cyanhd_type___6,
    encounter_information.pmh_cyanhd_type___7,
    encounter_information.other_cyanhd,
    encounter_information.pmh_ncyanhd,
    encounter_information.pmh_ncyanhd_type___1,
    encounter_information.pmh_ncyanhd_type___2,
    encounter_information.pmh_ncyanhd_type___3,
    encounter_information.pmh_ncyanhd_type___4,
    encounter_information.pmh_ncyanhd_type___5,
    encounter_information.pmh_ncyanhd_type___6,
    encounter_information.pmh_ncyanhd_type___7,
    encounter_information.pmh_ncyanhd_type___8,
    encounter_information.other_ncyanhd,
    encounter_information.pmh_pulmhtn,
    encounter_information.pmh_premature,
    encounter_information.pmh_premat_type,
    encounter_information.pmh_parlungdz,
    encounter_information.pmh_parlungdz_type___1,
    encounter_information.pmh_parlungdz_type___2,
    encounter_information.pmh_parlungdz_type___3,
    encounter_information.pmh_parlungdz_type___4,
    encounter_information.pmh_parlungdz_type___5,
    encounter_information.pmh_parlungdz_type___6,
    encounter_information.other_parlungdz,
    encounter_information.pmh_lowlungdz,
    encounter_information.pmh_lowlungdz_type___1,
    encounter_information.pmh_lowlungdz_type___2,
    encounter_information.pmh_lowlungdz_type___3,
    encounter_information.pmh_lowlungdz_type___4,
    encounter_information.pmh_lowlungdz_type___5,
    encounter_information.pmh_lowlungdz_type___6,
    encounter_information.pmh_lowlungdz_type___7,
    encounter_information.other_lowlungdz,
    encounter_information.pmh_uadz,
    encounter_information.pmh_uadz_type___1,
    encounter_information.pmh_uadz_type___2,
    encounter_information.pmh_uadz_type___3,
    encounter_information.pmh_uadz_type___4,
    encounter_information.pmh_uadz_type___5,
    encounter_information.other_uadz,
    encounter_information.pmh_respsupp,
    encounter_information.pmh_respsupp_type,
    encounter_information.pmh_oncdz,
    encounter_information.pmh_oncdz_treat,
    encounter_information.pmh_oncdz_type___1,
    encounter_information.pmh_oncdz_type___2,
    encounter_information.pmh_oncdz_type___3,
    encounter_information.pmh_oncdz_type___4,
    encounter_information.pmh_oncdz_type___5,
    encounter_information.pmh_oncdz_type___6,
    encounter_information.pmh_oncdz_type___7,
    encounter_information.pmh_oncdz_type___8,
    encounter_information.other_oncdz,
    encounter_information.pmh_bmt,
    encounter_information.pmh_bmt_type,
    encounter_information.pmh_bmt_days,
    encounter_information.pmh_imm_comp,
    encounter_information.pmh_imm_comp_type___1,
    encounter_information.pmh_imm_comp_type___2,
    encounter_information.pmh_imm_comp_type___3,
    encounter_information.pmh_imm_comp_type___4,
    encounter_information.pmh_imm_comp_type___5,
    encounter_information.pmh_imm_comp_type___6,
    encounter_information.pmh_imm_comp_type___7,
    encounter_information.pmh_imm_comp_type___8,
    encounter_information.pmh_imm_comp_type___9,
    encounter_information.other_imm_comp_type,
    encounter_information.primarydxicuadmit___1,
    encounter_information.primarydxicuadmit___2,
    encounter_information.primarydxicuadmit___3,
    encounter_information.primarydxicuadmit___4,
    encounter_information.primarydxicuadmit___5,
    encounter_information.primarydxicuadmit___6,
    encounter_information.primarydxicuadmit___7,
    encounter_information.primarydxicuadmit___8,
    encounter_information.primarydxicuadmit___9,
    encounter_information.primarydxicuadmit___10,
    encounter_information.primarydxicuadmit___11,
    encounter_information.primarydxicuadmit___12,
    encounter_information.primarydxicuadmit___13,
    encounter_information.other_med_admin,
    encounter_information.other_surg_admin,
    intubation_course.intubation_datetime,
    intubation_course.time_admit_ti,
    intubation_course.age_y_intubation,
    intubation_course.n4k_encounter,
    intubation_course.indication_intubation___1,
    intubation_course.indication_intubation___2,
    intubation_course.indication_intubation___3,
    intubation_course.indication_intubation___4,
    intubation_course.indication_intubation___5,
    intubation_course.indication_intubation___6,
    intubation_course.indication_intubation___7,
    intubation_course.indication_intubation___8,
    intubation_course.indication_intubation___9,
    intubation_course.indication_intubation___10,
    intubation_course.indication_intubation___11,
    intubation_course.indication_intubation___99,
    intubation_course.indication_intubation___12,
    intubation_course.other_ti_indication,
    intubation_course.uao_location,
    intubation_course.intubation_weight,
    intubation_course.intubation_height,
    intubation_course.ett_size,
    intubation_course.ett_cuff,
    intubation_course.ett_location,
    intubation_course.cardiacsurg,
    intubation_course.int_attempts,
    intubation_course.any_extub_attempt,
    intubation_course.why_never_ext,
    intubation_course.extubation_weight,
    intubation_course.extubation_height,
    intubation_course.extubation_datetime,
    intubation_course.length_ti,
    intubation_course.ext_loc,
    intubation_course.ext_codestatus,
    intubation_course.ext_type_planned,
    intubation_course.risk_perception,
    intubation_course.diff_airway,
    intubation_course.sed_score_type,
    intubation_course.other_sed_score,
    intubation_course.sbs_score,
    intubation_course.comfortb_score,
    intubation_course.rass_score,
    intubation_course.gcs,
    intubation_course.sbt_done,
    intubation_course.minimalvent_nonsbt,
    intubation_course.pip,
    intubation_course.set_rr,
    intubation_course.set_peep,
    intubation_course.fio2,
    intubation_course.set_ps,
    intubation_course.sbt_support_strategy,
    intubation_course.peep_cpap,
    intubation_course.ps_sbt,
    intubation_course.sbt_fio2,
    intubation_course.sbt_duration,
    intubation_course.sbt_result,
    intubation_course.sbt_fail_reason___1,
    intubation_course.sbt_fail_reason___2,
    intubation_course.sbt_fail_reason___3,
    intubation_course.sbt_fail_reason___4,
    intubation_course.sbt_fail_reason___5,
    intubation_course.sbt_fail_reason___6,
    intubation_course.sbt_fail_reason___7,
    intubation_course.sbt_fail_reason___8,
    intubation_course.sbt_fail_reason___9,
    intubation_course.other_sbt_fail,
    intubation_course.airleak,
    intubation_course.airleak_pressure,
    intubation_course.nifpimax,
    intubation_course.nifpimax_value,
    intubation_course.steroids,
    intubation_course.steroid_type,
    intubation_course.steroid_dose,
    intubation_course.steroid_frequency,
    intubation_course.steroid_timing,
    intubation_course.plan_ext_initial_nrs,
    intubation_course.unplan_ext_initial_nrs,
    intubation_course.unplan_nrs,
    intubation_course.unplan_nrs_type___1,
    intubation_course.unplan_nrs_type___2,
    intubation_course.unplan_nrs_type___3,
    intubation_course.unplan_nrs_type___4,
    intubation_course.hfnc_initial_flow,
    intubation_course.hfnc_max_flow,
    intubation_course.course_exposures___1,
    intubation_course.course_exposures___2,
    intubation_course.course_exposures___3,
    intubation_course.course_exposures___4,
    intubation_course.course_exposures___5,
    intubation_course.course_exposures___6,
    intubation_course.course_exposures___7,
    intubation_course.ext_attempt_outcome,
    intubation_course.ext_fail_causes___1,
    intubation_course.ext_fail_causes___2,
    intubation_course.ext_fail_causes___3,
    intubation_course.ext_fail_causes___4,
    intubation_course.ext_fail_causes___5,
    intubation_course.ext_fail_causes___6,
    intubation_course.ext_fail_causes___7,
    intubation_course.ext_fail_causes___8,
    intubation_course.ext_fail_causes___9,
    intubation_course.ext_fail_causes___10,
    intubation_course.ext_fail_causes___11,
    intubation_course.ext_fail_causes___12,
    intubation_course.ext_fail_causes___13,
    intubation_course.ext_fail_causes___14,
    intubation_course.ext_fail_causes___15,
    intubation_course.other_ext_fail_cause,
    intubation_course.ext_fail_uao_loc,
    intubation_course.qualify_transfer,
    encounter_outcomes.icu_end_datetime,
    encounter_outcomes.icu_disposition,
    encounter_outcomes.time_extub_dispo,
    encounter_outcomes.icu_los,
    encounter_information.encounter_information_complete,
    intubation_course.intubation_course_complete,
    encounter_outcomes.encounter_outcomes_complete
from
    encounter_information
    inner join intubation_course
        on encounter_information.local_encounter_id = intubation_course.local_encounter_id
    inner join encounter_outcomes
        on encounter_information.local_encounter_id = encounter_outcomes.local_encounter_id
