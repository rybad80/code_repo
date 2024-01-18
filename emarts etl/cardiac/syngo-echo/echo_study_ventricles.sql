with echos as (
    select
        source_system_id,
        echo_study_id
    from {{ ref('echo_study') }}
    where lower(source_system) = 'syngo'
),
measurements as (
    select
        studyid,
        cast(avg(case when lower(name) = 'lv_mass_index_ht_2_7_m_mode_ase_corr_calc' then floatvalue end) as decimal (28, 15)) as left_ventricle_mass_index
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('lv_mass_index_ht_2_7_m_mode_ase_corr_calc')
    group by
        studyid
),
observation_display_values as (
    select
        syngo_echo_observationvalue.studyid,
        lower(syngo_echo_observationname.name) as observation_name,
        isnull(cast(syngo_echo_observationfieldmap.worksheetvalue as varchar(400)),
            cast(syngo_echo_observationvalue.val as varchar(400))) as displayvalue
    from {{ source('syngo_echo_ods', 'syngo_echo_observationvalue') }} as syngo_echo_observationvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_observationname') }} as syngo_echo_observationname
       on syngo_echo_observationvalue.observationid = syngo_echo_observationname.id
    left join {{ source('syngo_echo_ods', 'syngo_echo_observationfieldmap') }} as syngo_echo_observationfieldmap
       on syngo_echo_observationfieldmap.observationname = syngo_echo_observationname.name
       and syngo_echo_observationfieldmap.databasevalue = syngo_echo_observationvalue.val
    where lower(syngo_echo_observationname.name) in ('chw_rv_systolic_function_obs', 'rv_structure_severity_obs',
        'nl_rv_size_and_fx_obs', 'septal_position_obs', 'chw_lv_systolic_function_obs', 'nl_lv_size_and_fx_obs',
        'lv_structure_severity_0_obs', 'no_regional_wall_motion_abnormalities_obs', 'vsd_1_location_size_chop_obs',
        'residual_vsd_chop_obs', 'residual_shunting_with_chop_obs', 'vsd_type_restriction_obs', 'vsd_shunting_obs')
),
observations as (
    select
        studyid,
        case when observation_name = 'chw_rv_systolic_function_obs' then displayvalue end as right_ventricle_systolic_function,
        case when observation_name = 'rv_structure_severity_obs' then displayvalue end as right_ventricle_structure_severity,
        case when observation_name = 'nl_rv_size_and_fx_obs' then displayvalue end as right_ventricle_size_fx,
        case when observation_name = 'septal_position_obs' then displayvalue end as right_ventricle_septal_position,
        case when observation_name = 'chw_lv_systolic_function_obs' then displayvalue end as left_ventricle_systolic_function,
        case when observation_name = 'nl_lv_size_and_fx_obs' then displayvalue end as left_ventricle_size_fx,
        case when observation_name = 'lv_structure_severity_0_obs' then displayvalue end as left_ventricle_structure_severity,
        case when observation_name = 'no_regional_wall_motion_abnormalities_obs' then displayvalue end as left_ventricle_regional_wall_motion,
        case when observation_name = 'vsd_1_location_size_chop_obs' then displayvalue end as vsd_type_size,
        case when observation_name = 'residual_vsd_chop_obs' then displayvalue end as vsd_residual_chop,
        case when observation_name = 'residual_shunting_with_chop_obs' then displayvalue end as vsd_residual_shunting,
        case when observation_name = 'vsd_type_restriction_obs' then displayvalue end as vsd_restriction,
        case when observation_name = 'vsd_shunting_obs' then displayvalue end as vsd_shunting
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        group_concat(right_ventricle_systolic_function, ';') as right_ventricle_systolic_function,
        group_concat(right_ventricle_structure_severity, ';') as right_ventricle_structure_severity,
        group_concat(right_ventricle_size_fx, ';') as right_ventricle_size_fx,
        group_concat(right_ventricle_septal_position, ';') as right_ventricle_septal_position
    from observations
    group by studyid
),
observation_group_2 as (
    select
        studyid,
        group_concat(left_ventricle_systolic_function, ';') as left_ventricle_systolic_function,
        group_concat(left_ventricle_size_fx, ';') as left_ventricle_size_fx,
        group_concat(left_ventricle_structure_severity, ';') as left_ventricle_structure_severity,
        group_concat(left_ventricle_regional_wall_motion, ';') as left_ventricle_regional_wall_motion
    from observations
    group by studyid
),
observation_group_3 as (
    select
        studyid,
        group_concat(vsd_type_size, ';') as vsd_type_size,
        group_concat(vsd_residual_chop, ';') as vsd_residual_chop,
        group_concat(vsd_residual_shunting, ';') as vsd_residual_shunting,
        group_concat(vsd_restriction, ';') as vsd_restriction
    from observations
    group by studyid
),
observation_group_4 as (
    select
        studyid,
        group_concat(vsd_shunting, ';') as vsd_shunting
    from observations
    group by studyid
),
sq_echo_study_ventricles as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(right_ventricle_systolic_function as varchar(255)) as right_ventricle_systolic_function,
        cast(right_ventricle_structure_severity as varchar(255)) as right_ventricle_structure_severity,
        cast(right_ventricle_size_fx as varchar(255)) as right_ventricle_size_fx,
        cast(right_ventricle_septal_position as varchar(255)) as right_ventricle_septal_position,
        cast(left_ventricle_systolic_function as varchar(255)) as left_ventricle_systolic_function,
        cast(left_ventricle_size_fx as varchar(255)) as left_ventricle_size_fx,
        cast(left_ventricle_structure_severity as varchar(255)) as left_ventricle_structure_severity,
        cast(left_ventricle_mass_index as numeric(28, 15)) as left_ventricle_mass_index,
        cast(left_ventricle_regional_wall_motion as varchar(255)) as left_ventricle_regional_wall_motion,
        cast(vsd_type_size as varchar(255)) as vsd_type_size,
        cast(vsd_residual_chop as varchar(255)) as vsd_residual_chop,
        cast(vsd_residual_shunting as varchar(255)) as vsd_residual_shunting,
        cast(vsd_restriction as varchar(255)) as vsd_restriction,
        cast(vsd_shunting as varchar(255)) as vsd_shunting
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    left join observation_group_1
        on echos.source_system_id = observation_group_1.studyid
    left join observation_group_2
        on echos.source_system_id = observation_group_2.studyid
    left join observation_group_3
        on echos.source_system_id = observation_group_3.studyid
    left join observation_group_4
        on echos.source_system_id = observation_group_4.studyid
where (right_ventricle_systolic_function is not null or right_ventricle_structure_severity is not null
    or right_ventricle_size_fx is not null or right_ventricle_septal_position is not null or left_ventricle_systolic_function is not null
    or left_ventricle_size_fx is not null or left_ventricle_structure_severity is not null or left_ventricle_mass_index is not null
    or left_ventricle_regional_wall_motion is not null or vsd_type_size is not null or vsd_residual_chop is not null
    or vsd_residual_shunting is not null or vsd_restriction is not null or vsd_shunting is not null)
)

select
    echo_study_id,
    right_ventricle_systolic_function,
    right_ventricle_structure_severity,
    right_ventricle_size_fx,
    right_ventricle_septal_position,
    left_ventricle_systolic_function,
    left_ventricle_size_fx,
    left_ventricle_structure_severity,
    left_ventricle_mass_index,
    left_ventricle_regional_wall_motion,
    vsd_type_size,
    vsd_residual_chop,
    vsd_residual_shunting,
    vsd_restriction,
    vsd_shunting
from sq_echo_study_ventricles
