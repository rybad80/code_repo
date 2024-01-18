with measurements_raw as (
select
    studyid,
    name,
    case when lower(name) = 'lv_fs_teich_2d_calc' then floatvalue end as lv_fs_teich_2d_calc,
    case when lower(name) = 'lvot_vmax_calc' then floatvalue end as lvot_vmax_calc,
    case when lower(name) = 'tr_vmax_calc' then floatvalue end as tr_vmax_calc
from
    {{source('ccis_ods', 'syngo_echo_measurementvalue')}} as measurementvalue
    inner join {{source('ccis_ods', 'syngo_echo_measurementtype')}} as measurementtype
        on measurementtype.id = measurementvalue.measurementtypeidx
where
    lower(name) in ('lv_fs_teich_2d_calc', 'lvot_vmax_calc', 'tr_vmax_calc')
    and instancenumber in (0, 1)
),

measurements_setup as (
select
    studyid,
    lv_fs_teich_2d_calc,
    lvot_vmax_calc,
    tr_vmax_calc
from
    measurements_raw
),

measurements as (
select
    studyid,
    avg(lv_fs_teich_2d_calc) as left_ventricle_fs_teich_2d_avg,
    avg(lvot_vmax_calc) as lvot_vmax_calc_avg,
    avg(tr_vmax_calc) as tricuspid_regurgitation_velocity_avg
from
    measurements_setup
group by
    studyid
),

observations as (
select
    studyid,
    max(case when lower(name) = 'fontan_procedure_obs'
                      then worksheetvalue end) as fontan_procedure,
    group_concat(case when lower(name) = 'protocol_requested_obs'
                      then worksheetvalue end, ';') as protocol_requested,
    group_concat(case when lower(name) = 'chop_proc_codes_4_obs'
                      then worksheetvalue end, ';') as chop_proc_codes,
    max(case when lower(name) = 'research_protocol_1_obs'
                      then worksheetvalue end) as research_protocol,
    group_concat(case when lower(name) = 'chw_ivc_side_obs'
                      then worksheetvalue end, ';') as chw_ivc_side,
    max(case when lower(name) = 'chw_asd_shunt_direction_obs'
                      then worksheetvalue end) as asd_shunt_direction
from
    {{source('ccis_ods', 'syngo_echo_observationvalue')}} as observationvalue
    inner join {{source('ccis_ods', 'syngo_echo_observationname')}} as observationname
      on observationvalue.observationid = observationname.id
    left join {{source('ccis_ods', 'syngo_echo_observationfieldmap')}} as observationmap
      on observationmap.observationname = observationname.name
      and observationmap.databasevalue = observationvalue.val
where
    lower(name) in ('fontan_procedure_obs', 'protocol_requested_obs', 'chop_proc_codes_4_obs',
                    'research_protocol_1_obs', 'chw_ivc_side_obs', 'chw_asd_shunt_direction_obs')
    and val != 'VALUESOURCE'
group by
    studyid
)

select
    cardiac_echo.cardiac_study_id,
    cardiac_echo.mrn,
    cardiac_echo.patient_name,
    cardiac_echo.study_date,
    advanced_3d_study,
    advanced_3d_valves,
    age_at_echo,
    aorta_ao_asc_avg,
    aorta_ao_asc_zscore,
    aorta_ascending_size,
    aorta_coarctation_mean_gradient_avg,
    aorta_coarct_corr_gradient_avg,
    aorta_coarct_type_severity,
    aorta_desc_peak_gradient_avg,
    aorta_desc_vmax_avg,
    aorta_flow,
    aorta_isthmus_diam_avg,
    aorta_isthmus_diam_zscore,
    aorta_pre_coarct_peak_gradient_avg,
    aorta_pre_coarct_vmax_avg,
    aorta_surgery_mean_gradient_avg,
    aorta_surgery_peak_gradient_avg,
    aorta_surgery_vmax_avg,
    aorta_transverse_diam_avg,
    aorta_transverse_diam_zscore,
    aortic_valve_anatomy,
    aortic_valve_ao_ann_diam_avg,
    aortic_valve_ao_ann_diam_zscore,
    aortic_valve_ao_root,
    aortic_valve_ao_root_calc_avg,
    aortic_valve_ao_root_calc_zscore,
    aortic_valve_ao_st_jnct_avg,
    aortic_valve_ao_st_jnct_zscore,
    aortic_valve_mean_gradient_avg,
    aortic_valve_neoaortic_obstruction,
    aortic_valve_neoaortic_valve_regurgitation,
    aortic_valve_neo_aov_gradient,
    aortic_valve_normal_doppler,
    aortic_valve_peak_gradient_avg,
    aortic_valve_regurgitation,
    aortic_valve_stenosis,
    aortic_valve_supravalvlar_stenosis,
    asd_shunt_direction,
    attending,
    avcanal_common_avv_regurgitation,
    avcanal_left_avv_regurgitation,
    avcanal_rastelli_type,
    avcanal_right_avv_regurgitation,
    avcanal_stenosis,
    avcanal_type,
    avcanal_unbalanced,
    av_canal_surgery_residual_left_avvr,
    av_canal_surgery_residual_left_avvs,
    av_canal_surgery_residual_right_avvr,
    av_canal_surgery_residual_right_avvs,
    bp_diastolic,
    bp_systolic,
    bsa,
    chop_proc_codes,
    chw_ivc_side,
    common_single_ventricle_av_valve_regurgitation,
    coronary_arteries_nmv,
    coronary_arteries_normal,
    coronary_arteries_not_eval,
    diagnosis_description,
    diastolic_function_e_e_prime_lateral_avg,
    diastolic_function_e_e_prime_medial_avg,
    diastolic_function_lateral_e_prime_avg,
    diastolic_function_medial_e_prime_avg,
    fellow,
    fontan_procedure,
    left_atrium_size,
    left_ventricle_ef_3d_avg,
    left_ventricle_ef_a4c_avg,
    left_ventricle_ef_bip,
    left_ventricle_ef_mmode_avg,
    left_ventricle_fs_teich_2d_avg,
    left_ventricle_mass_index,
    left_ventricle_mass_mmode_avg,
    left_ventricle_mass_zscore,
    left_ventricle_mmode_ivs_d_avg,
    left_ventricle_mmode_ivs_d_zscore,
    left_ventricle_mmode_ivs_s_avg,
    left_ventricle_mmode_ivs_s_zscore,
    left_ventricle_mmode_lvid_d_avg,
    left_ventricle_mmode_lvid_d_zscore,
    left_ventricle_mmode_lvid_s_avg,
    left_ventricle_mmode_lvid_s_zscore,
    left_ventricle_mmode_lvpwd_avg,
    left_ventricle_mmode_lvpwd_zscore,
    left_ventricle_mmode_lvpws_avg,
    left_ventricle_mmode_lvpws_zscore,
    left_ventricle_mmode_lvsf,
    left_ventricle_regional_wall_motion,
    left_ventricle_size_fx,
    left_ventricle_structure_severity,
    left_ventricle_systolic_function,
    left_ventricle_volume_d_avg,
    left_ventricle_volume_s_avg,
    lvot_mean_gradient_avg,
    lvot_obstruction,
    lvot_obstruction_severity_type,
    lvot_peak_gradient_avg,
    lvot_vmax_calc_avg,
    mitral_ann_diam_d_4ac_avg,
    mitral_ann_diam_d_4ac_zscore,
    mitral_ann_diam_d_ap_avg,
    mitral_ann_diam_d_ap_zscore,
    mitral_area_avg,
    mitral_a_v_max_avg,
    mitral_e_a_inflow_avg,
    mitral_e_v_max_avg,
    mitral_mean_gradient_avg,
    mitral_regurgitation,
    mitral_stenosis,
    neo_pulmonary_obstruction,
    neo_pulmonary_regurgitation,
    number_of_images,
    patient_height_cm,
    patient_weight_kg,
    pda_ampulla_orifince_dimen_avg,
    pda_flow_restriction,
    pda_mean_gradient_avg,
    pda_peak_gradient_avg,
    pda_shunt_direction,
    pda_type_size,
    pda_vmax_avg,
    pericardial_effusion,
    pericardial_location,
    pericardial_size,
    protocol_requested,
    pulmonary_arteries_branches_normal,
    pulmonary_arteries_lpa_diam_avg,
    pulmonary_arteries_lpa_diam_zscore,
    pulmonary_arteries_lpa_peak_gradient_avg,
    pulmonary_arteries_lpa_structure_severity,
    pulmonary_arteries_lpa_vmax_avg,
    pulmonary_arteries_rpa_diam_avg,
    pulmonary_arteries_rpa_diam_zscore,
    pulmonary_arteries_rpa_peak_gradient_avg,
    pulmonary_arteries_rpa_structure_severity,
    pulmonary_arteries_rpa_vmax_avg,
    pulmonary_arteries_supravalvular_stenosis,
    pulmonary_conduit_regurgitation,
    pulmonary_conduit_stenosis,
    pulmonary_normal_doppler,
    pulmonary_regurgitation,
    pulmonary_stenosis,
    pulmonary_valve_mean_gradient_avg,
    pulmonary_valve_peak_gradient_avg,
    qi_additional_images,
    qi_communication,
    qi_order,
    qi_patient_factors,
    qi_project_name,
    research_protocol,
    residual_asd,
    right_atrium_size,
    right_ventricle_e_e_prime_avg,
    right_ventricle_e_free_wall_avg,
    right_ventricle_septal_position,
    right_ventricle_size_fx,
    right_ventricle_structure_severity,
    right_ventricle_systolic_function,
    right_ventricle_tapse_2d_avg,
    right_ventricle_tapse_mmode_avg,
    rvot_mean_gradient_avg,
    rvot_obstruction,
    rvot_obstruction_severity_type,
    rvot_peak_gradient_avg,
    sonographer,
    time_on_last_image,
    tricuspid_ann_diam_d_4ac_avg,
    tricuspid_ann_diam_d_4ac_zscore,
    tricuspid_ann_diam_d_ap_avg,
    tricuspid_ann_diam_d_ap_zscore,
    tricuspid_area_avg,
    tricuspid_area_peak_gradient_avg,
    tricuspid_ebsteins_anomaly,
    tricuspid_mean_gradient_avg,
    tricuspid_regurgitation,
    tricuspid_regurgitation_velocity_avg,
    tricuspid_stenosis,
    volume_d_4chamber_avg,
    volume_index_d_4chamber_avg,
    volume_index_s_4chamber_avg,
    volume_s_4chamber_avg,
    vsd_residual_chop,
    vsd_residual_shunting,
    vsd_restriction,
    vsd_shunting,
    vsd_type_size
from
    {{ref('cardiac_echo')}} as cardiac_echo
    inner join {{source('cdw', 'echo_study')}} as echo_study
      on cardiac_echo.cardiac_study_id = echo_study.echo_study_id
    left join {{source('cdw', 'echo_study_aorta_calcs')}} as aorta_calc
      on echo_study.echo_study_id = aorta_calc.echo_study_id
    left join {{source('cdw', 'echo_study_aortic_valve_calcs')}} as aortic_valve_calcs
      on echo_study.echo_study_id = aortic_valve_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_arteries')}} as arteries
      on echo_study.echo_study_id = arteries.echo_study_id
    left join {{source('cdw', 'echo_study_atria')}} as atria
      on echo_study.echo_study_id = atria.echo_study_id
    left join {{source('cdw', 'echo_study_demographics')}} as demographics
      on echo_study.echo_study_id = demographics.echo_study_id
    left join {{source('cdw', 'echo_study_inlets')}} as inlets
      on echo_study.echo_study_id = inlets.echo_study_id
    left join {{source('cdw', 'echo_study_left_ventricle_calcs')}} as left_ventricle_calcs
      on echo_study.echo_study_id = left_ventricle_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_lvot_calcs')}} as lvot_calcs
      on echo_study.echo_study_id = lvot_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_mitral_calcs')}} as mitral_calcs
      on echo_study.echo_study_id = mitral_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_other')}} as other
      on echo_study.echo_study_id = other.echo_study_id
    left join {{source('cdw', 'echo_study_outlets')}} as outlets
      on echo_study.echo_study_id = outlets.echo_study_id
    left join {{source('cdw', 'echo_study_pda_calcs')}} as pda_calcs
      on echo_study.echo_study_id = pda_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_pulmonary_arteries_calcs')}} as pulmonary_arteries_calcs
      on echo_study.echo_study_id = pulmonary_arteries_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_pulmonary_valve_calcs')}} as pulmonary_valve_calcs
      on echo_study.echo_study_id = pulmonary_valve_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_quality_improvement')}} as quality_improvement
      on echo_study.echo_study_id = quality_improvement.echo_study_id
    left join {{source('cdw', 'echo_study_right_ventricle_calcs')}} as right_ventricle_calcs
      on echo_study.echo_study_id = right_ventricle_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_rvot_calcs')}} as rvot_calcs
      on echo_study.echo_study_id = rvot_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_surgeries')}} as surgeries
      on echo_study.echo_study_id = surgeries.echo_study_id
    left join {{source('cdw', 'echo_study_tricuspid_calcs')}} as tricuspid_calcs
      on echo_study.echo_study_id = tricuspid_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_ventricles')}} as ventricles
      on echo_study.echo_study_id = ventricles.echo_study_id
    left join measurements
      on echo_study.source_system_id = measurements.studyid
    left join observations
      on echo_study.source_system_id = observations.studyid
