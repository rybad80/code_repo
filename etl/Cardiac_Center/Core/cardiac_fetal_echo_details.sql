select
    cardiac_echo.cardiac_study_id,
    cardiac_echo.mrn,
    cardiac_echo.patient_name,
    cardiac_echo.study_date,
    abdominal_circumference_avg,
    abdominal_circumference_ma_avg,
    abdominal_circumference_ma_sd,
    age_at_echo,
    amniotic_fluid,
    anchor_study_date,
    aorta_arch_diagnosis_severity,
    aorta_arch_sidedness,
    aorta_coarct_type_severity,
    aorta_trans_arch_hypoplasia,
    aortic_valve_anatomy,
    aortic_valve_normal,
    aortic_valve_regurgitation,
    aortic_valve_root_diliation,
    aortic_valve_stenosis,
    assigned_estimated_delivery_date,
    assigned_estimated_delivery_date_method,
    assigned_menstrual_age,
    atrial_septal_defect_type_size,
    atrial_septum,
    attending,
    avcanal_common_avv_regurgitation,
    avcanal_type,
    biparietal_diameter_avg,
    biparietal_diameter_ma_avg,
    biparietal_diameter_ma_sd,
    bp_diastolic,
    bp_systolic,
    bsa,
    cardiovasc_cardiac_enlargement,
    cardiovasc_ductus_venosus,
    cardiovasc_mitral_regurgitation,
    cardiovasc_mv_inflow,
    cardiovasc_outflow_tracts,
    cardiovasc_pulmonary_insufficiency,
    cardiovasc_systolic_dysfunction,
    cardiovasc_total_score_avg,
    cardiovasc_tricuspid_regurgitation,
    cardiovasc_tv_inflow,
    cardiovasc_twin_twin_comment,
    cardiovasc_umbilical_artery,
    cardiovasc_umbilical_vein,
    cardiovasc_ventricular_hypertrophy,
    clinical_estimated_delivery_date,
    clinical_menstrual_age,
    combined_cardiac_output_avg,
    combined_cardiac_output_comment,
    common_single_ventricle_morphology,
    composite_ma_avg,
    composite_ma_sd,
    conotruncus_aortopulmonary_window,
    conotruncus_double_outlet_left_ventricle,
    conotruncus_double_outlet_right_ventricle,
    conotruncus_d_tga,
    conotruncus_normal_conotruncal_anatomy,
    conotruncus_rv_aorta_vsd,
    conotruncus_tetralogy_of_fallot,
    conotruncus_truncus_arteriosus_type,
    diagnosis_description,
    fetus.estimated_fetal_weight_avg,
    estimated_fetal_weight_ma_sd,
    femur_length_avg,
    femur_length_ma_avg,
    femur_length_ma_sd,
    fetal_doppler_absent_ductus_venosus,
    fetal_doppler_site_1_end_diastolic_volume_avg,
    fetal_doppler_site_1_name,
    fetal_doppler_site_1_peak_systolic_velocity_avg,
    fetal_doppler_site_1_pulsatility_index_avg,
    fetal_doppler_site_1_time_average_mean_avg,
    fetal_doppler_site_2_end_diastolic_volume_avg,
    fetal_doppler_site_2_name,
    fetal_doppler_site_2_peak_systolic_velocity_avg,
    fetal_doppler_site_2_pulsatility_index_avg,
    fetal_doppler_site_2_time_average_mean_avg,
    fetal_doppler_site_3_end_diastolic_volume_avg,
    fetal_doppler_site_3_name,
    fetal_doppler_site_3_peak_systolic_velocity_avg,
    fetal_doppler_site_3_pulsatility_index_avg,
    fetal_doppler_site_3_time_average_mean_avg,
    gravida,
    head_circumference_avg,
    head_circumference_ma_avg,
    head_circumference_ma_sd,
    heart_rate_avg,
    heart_rate_rhythm,
    heart_rate_rhythm_comment,
    imaging_diagnosis,
    left_annulus_avg,
    left_atrium_size,
    left_cardiac_output_avg,
    left_velocity_time_integral_avg,
    left_ventricle_size_fx,
    left_ventricle_structure_severity,
    left_ventricle_systolic_function,
    lmp_date,
    lvot_obstruction,
    lvot_obstruction_severity_type,
    mitral_regurgitation,
    mitral_stenosis,
    mitral_structure_severity,
    normal_fetal_anatomy,
    normal_umbilical_cord_anatomy,
    no_lmp_reason,
    para,
    patent_foramen_ovale,
    patient_height_cm,
    patient_weight_kg,
    pda_type_size,
    placenta_appearance,
    pregnancy_history_comment,
    procedure as procedure_name,
    pulmonary_arteries_branches_normal,
    pulmonary_arteries_lpa_structure_severity,
    pulmonary_arteries_mpa_structure_severity,
    pulmonary_arteries_normal,
    pulmonary_arteries_rpa_structure_severity,
    pulmonary_atresia,
    pulmonary_regurgitation,
    pulmonary_stenosis,
    pulmonary_veins_normal,
    pulmonary_veins_papvc,
    pulmonary_veins_tapvc,
    referring_physician,
    right_annulus_avg,
    right_atrium_size,
    right_cardiac_output_avg,
    right_velocity_time_integral_avg,
    right_ventricle_diastolic_function,
    right_ventricle_size_fx,
    right_ventricle_structure_severity,
    right_ventricle_systolic_function,
    rvot_obstruction,
    rvot_obstruction_severity_type,
    situs_atrial,
    situs_cardiac_position,
    situs_cardiotype_undetermined,
    situs_great_artery_relationships,
    situs_ventricular_loop,
    situs_visceral_situs,
    sonographer,
    systemic_veins_coronary_sinus,
    systemic_veins_inferior_vena_cava,
    systemic_veins_left_superior_vena_cava,
    systemic_veins_right_superior_vena_cava,
    tricuspid_atresia,
    tricuspid_ebsteins_anomaly,
    tricuspid_normal_doppler,
    tricuspid_regurgitation,
    tricuspid_structure_severity,
    type_of_gestation,
    vsd_not_observed,
    vsd_type_size
from
    {{ref('cardiac_echo')}} as cardiac_echo
    inner join {{source('cdw', 'echo_fetal_study')}} as study
      on cardiac_echo.cardiac_study_id = study.echo_fetal_study_id
    inner join {{source('cdw', 'echo_fetal_study_demographics')}} as demographics
      on demographics.echo_fetal_study_id = study.echo_fetal_study_id
    inner join {{source('cdw', 'echo_fetal_study_fetus')}} as fetus
      on fetus.echo_fetal_study_id = study.echo_fetal_study_id
    left join {{source('cdw', 'echo_fetal_study_pregnancy')}} as pregnancy
      on pregnancy.echo_fetal_study_id = study.echo_fetal_study_id
    left join {{source('cdw', 'echo_fetal_study_arteries')}} as arteries
      on arteries.echo_fetal_study_id = study.echo_fetal_study_id
      and arteries.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_atria')}} as atria
      on atria.echo_fetal_study_id = study.echo_fetal_study_id
      and atria.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_cardiovasc')}} as cardiovasc
      on cardiovasc.echo_fetal_study_id = study.echo_fetal_study_id
      and cardiovasc.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_combined_cardiac')}} as combined_cardiac
      on combined_cardiac.echo_fetal_study_id = study.echo_fetal_study_id
      and combined_cardiac.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_conotruncus')}} as contruncus
      on contruncus.echo_fetal_study_id = study.echo_fetal_study_id
      and contruncus.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_fetal_doppler')}} as doppler
      on doppler.echo_fetal_study_id = study.echo_fetal_study_id
      and doppler.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_inlets')}} as inlets
      on inlets.echo_fetal_study_id = study.echo_fetal_study_id
      and inlets.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_outlets')}} as outlets
      on outlets.echo_fetal_study_id = study.echo_fetal_study_id
      and outlets.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_veins')}} as veins
      on veins.echo_fetal_study_id = study.echo_fetal_study_id
      and veins.owner_id = fetus.owner_id
    left join {{source('cdw', 'echo_fetal_study_ventricles')}} as ventricles
      on ventricles.echo_fetal_study_id = study.echo_fetal_study_id
      and ventricles.owner_id = fetus.owner_id
