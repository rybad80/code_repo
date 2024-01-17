with patients as (
    select distinct
        pat_key,
        'Enrolled' as status
    from
        {{source('cdw', 'patient_list_info')}} as patient_list_info
        inner join {{source('cdw', 'patient_list')}} as patient_list
            on patient_list.pat_lst_info_key = patient_list_info.pat_lst_info_key
        where
            lower(display_nm) = 'cardiac research registry - kim lin, md'
),
calcs as (
	select
        studyid,
        group_concat(lv_fs_teich_2d_calc, ';') as lv_fs_teich_2d_calc,
        group_concat(lvot_vmax_calc, ';') as lvot_vmax_calc
	from (
        select
            studyid,
            lv_fs_teich_2d_calc,
            lvot_vmax_calc
        from(
            select
                studyid,
                name,
                case when name = 'lv_fs_teich_2d_calc' then floatvalue end as lv_fs_teich_2d_calc,
                case when name = 'lvot_vmax_calc' then floatvalue end as lvot_vmax_calc
            from
                {{source('ccis_ods', 'syngo_echo_measurementvalue')}}
                inner join {{source('ccis_ods', 'syngo_echo_measurementtype')}}
                    on id = measurementtypeidx
                    and lower(name) in ('lv_fs_teich_2d_calc', 'lvot_vmax_calc')
        ) as c --noqa: L025
    ) as finaldata --noqa: L025
	group by studyid
)
select
    echo_study.patient_key as pat_key,
    echo_study.study_date_key,
    echo_study_left_ventricle_calcs.left_ventricle_mmode_lvpwd_avg,
    echo_study_left_ventricle_calcs.left_ventricle_mmode_ivs_d_avg,
    echo_study_left_ventricle_calcs.left_ventricle_mmode_ivs_d_zscore,
    echo_study_left_ventricle_calcs.left_ventricle_mmode_lvid_d_avg,
    echo_study_left_ventricle_calcs.left_ventricle_mmode_lvid_d_zscore,
    echo_study_left_ventricle_calcs.left_ventricle_mmode_lvid_s_avg,
    echo_study_left_ventricle_calcs.left_ventricle_mmode_lvid_s_zscore,
    echo_study_ventricles.left_ventricle_structure_severity,
    echo_study_outlets.lvot_obstruction_severity_type,
    echo_study_left_ventricle_calcs.left_ventricle_ef_bip,
    echo_study_left_ventricle_calcs.left_ventricle_ef_3d_avg,
    calcs.lv_fs_teich_2d_calc,
    echo_study_outlets.aortic_valve_regurgitation,
    echo_study_inlets.mitral_regurgitation,
    echo_study_outlets.pulmonary_regurgitation,
    echo_study_inlets.tricuspid_regurgitation,
    echo_study_left_ventricle_calcs.left_ventricle_mass_mmode_avg,
    echo_study_ventricles.left_ventricle_mass_index,
    calcs.lvot_vmax_calc,
    echo_study_lvot_calcs.lvot_peak_gradient_avg,
    echo_study_left_ventricle_calcs.diastolic_function_e_e_prime_lateral_avg,
    echo_study_left_ventricle_calcs.diastolic_function_medial_e_prime_avg,
    echo_study_left_ventricle_calcs.diastolic_function_e_e_prime_medial_avg,
    echo_study_mitral_calcs.mitral_e_v_max_avg,
    patients.status as study_status,
    echo_study.echo_study_id
from
    {{source('cdw', 'echo_study')}} as echo_study
    inner join patients
        on echo_study.patient_key = patients.pat_key
    inner join {{source('cdw', 'patient')}} as patient
        on echo_study.patient_key = patient.pat_key
    left join calcs
        on echo_study.source_system_id = calcs.studyid
    left join {{source('cdw', 'echo_study_ventricles')}} as echo_study_ventricles
        on echo_study.echo_study_id = echo_study_ventricles.echo_study_id
    left join {{source('cdw', 'echo_study_outlets')}} as echo_study_outlets
        on echo_study.echo_study_id = echo_study_outlets.echo_study_id
    left join {{source('cdw', 'echo_study_inlets')}} as echo_study_inlets
        on echo_study.echo_study_id = echo_study_inlets.echo_study_id
    left join {{source('cdw', 'echo_study_lvot_calcs')}} as echo_study_lvot_calcs
        on echo_study.echo_study_id = echo_study_lvot_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_mitral_calcs')}} as echo_study_mitral_calcs
        on echo_study.echo_study_id = echo_study_mitral_calcs.echo_study_id
    left join {{source('cdw', 'echo_study_left_ventricle_calcs')}} as echo_study_left_ventricle_calcs
        on echo_study.echo_study_id = echo_study_left_ventricle_calcs.echo_study_id
