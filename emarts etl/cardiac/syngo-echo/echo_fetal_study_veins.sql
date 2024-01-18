with fetal_echos as (
    select
        source_system_id,
        echo_fetal_study_id
    from {{ ref('echo_fetal_study') }}
    where lower(source_system) = 'syngo'
),

observation_display_values as (
    select
        syngo_echo_observationvalue.studyid,
        syngo_echo_observationvalue.ownerid,
        lower(syngo_echo_observationname.name) as observation_name,
        isnull(cast(syngo_echo_fetalobservationfieldmap.worksheetvalue as varchar(400)),
            cast(syngo_echo_observationvalue.val as varchar(400))) as displayvalue
    from {{ source('syngo_echo_ods', 'syngo_echo_observationvalue') }} as syngo_echo_observationvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_observationname') }} as syngo_echo_observationname
        on syngo_echo_observationvalue.observationid = syngo_echo_observationname.id
    left join {{ source('syngo_echo_ods', 'syngo_echo_fetalobservationfieldmap') }} as syngo_echo_fetalobservationfieldmap
        on syngo_echo_fetalobservationfieldmap.observationname = syngo_echo_observationname.name
        and syngo_echo_fetalobservationfieldmap.databasevalue = syngo_echo_observationvalue.val
    where lower(syngo_echo_observationname.name) in ('chw_sit_cardiotype_nwv_obs', 'chw_pvn_normal_obs',
        'chw_atrial_situs_1_obs', 'chw_great_artery_situs_obs', 'tapvc_obs', 'papvc_obs', 'chw_lsvc_obs',
        'chw_sit_cardiac_position_obs', 'chw_rsvc_obs', 'visceral_situs_obs', 'chw_ivc_side_obs',
        'chw_vent_loop_obs', 'coronary_sinus_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'chw_sit_cardiotype_nwv_obs' then displayvalue end as chw_sit_cardiotype_nwv_obs,
        case when observation_name = 'chw_pvn_normal_obs' then displayvalue end as chw_pvn_normal_obs,
        case when observation_name = 'chw_atrial_situs_1_obs' then displayvalue end as chw_atrial_situs_1_obs,
        case when observation_name = 'chw_great_artery_situs_obs' then displayvalue end as chw_great_artery_situs_obs,
        case when observation_name = 'tapvc_obs' then displayvalue end as tapvc_obs,
        case when observation_name = 'papvc_obs' then displayvalue end as papvc_obs,
        case when observation_name = 'chw_lsvc_obs' then displayvalue end as chw_lsvc_obs,
        case when observation_name = 'chw_sit_cardiac_position_obs' then displayvalue end as chw_sit_cardiac_position_obs,
        case when observation_name = 'chw_rsvc_obs' then displayvalue end as chw_rsvc_obs,
        case when observation_name = 'visceral_situs_obs' then displayvalue end as visceral_situs_obs,
        case when observation_name = 'chw_ivc_side_obs' then displayvalue end as chw_ivc_side_obs,
        case when observation_name = 'chw_vent_loop_obs' then displayvalue end as chw_vent_loop_obs,
        case when observation_name = 'coronary_sinus_obs' then displayvalue end as coronary_sinus_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(chw_sit_cardiotype_nwv_obs, ';') as chw_sit_cardiotype_nwv_obs,
        group_concat(chw_pvn_normal_obs, ';') as chw_pvn_normal_obs,
        group_concat(chw_atrial_situs_1_obs, ';') as chw_atrial_situs_1_obs,
        group_concat(chw_great_artery_situs_obs, ';') as chw_great_artery_situs_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(tapvc_obs, ';') as tapvc_obs,
        group_concat(papvc_obs, ';') as papvc_obs,
        group_concat(chw_lsvc_obs, ';') as chw_lsvc_obs,
        group_concat(chw_sit_cardiac_position_obs, ';') as chw_sit_cardiac_position_obs
    from observations
    group by studyid, ownerid
),

observation_group_3 as (
    select
        studyid,
        ownerid,
        group_concat(chw_rsvc_obs, ';') as chw_rsvc_obs,
        group_concat(visceral_situs_obs, ';') as visceral_situs_obs,
        group_concat(chw_ivc_side_obs, ';') as chw_ivc_side_obs,
        group_concat(chw_vent_loop_obs, ';') as chw_vent_loop_obs
    from observations
    group by studyid, ownerid
),

observation_group_4 as (
    select
        studyid,
        ownerid,
        group_concat(coronary_sinus_obs, ';') as coronary_sinus_obs
    from observations
    group by studyid, ownerid
),

sq_echo_fetal_study_veins as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        chw_pvn_normal_obs as pulmonary_veins_normal,
        papvc_obs as pulmonary_veins_papvc,
        tapvc_obs as pulmonary_veins_tapvc,
        chw_sit_cardiotype_nwv_obs as situs_cardiotype_undetermined,
        chw_atrial_situs_1_obs as situs_atrial,
        chw_vent_loop_obs as situs_ventricular_loop,
        chw_great_artery_situs_obs as situs_great_artery_relationships,
        chw_sit_cardiac_position_obs as situs_cardiac_position,
        visceral_situs_obs as situs_visceral_situs,
        coronary_sinus_obs as systemic_veins_coronary_sinus,
        chw_ivc_side_obs as systemic_veins_inferior_vena_cava,
        chw_lsvc_obs as systemic_veins_left_superior_vena_cava,
        chw_rsvc_obs as systemic_veins_right_superior_vena_cava
    from fetal_echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study_owner') }} as syngo_echo_study_owner
        on fetal_echos.source_system_id = syngo_echo_study_owner.study_ref
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
        and syngo_echo_study_owner.ownerid = observation_group_1.ownerid
    left join observation_group_2
        on fetal_echos.source_system_id = observation_group_2.studyid
        and syngo_echo_study_owner.ownerid = observation_group_2.ownerid
    left join observation_group_3
        on fetal_echos.source_system_id = observation_group_3.studyid
        and syngo_echo_study_owner.ownerid = observation_group_3.ownerid
    left join observation_group_4
        on fetal_echos.source_system_id = observation_group_4.studyid
        and syngo_echo_study_owner.ownerid = observation_group_4.ownerid
where ownertype in (1, 2)
and (coronary_sinus_obs is not null or papvc_obs is not null or tapvc_obs is not null or visceral_situs_obs is not null or chw_ivc_side_obs is not null or chw_lsvc_obs is not null or chw_pvn_normal_obs is not null or chw_rsvc_obs is not null or chw_sit_cardiac_position_obs is not null or chw_sit_cardiotype_nwv_obs is not null or chw_atrial_situs_1_obs is not null or chw_great_artery_situs_obs is not null or chw_vent_loop_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(pulmonary_veins_normal as varchar(255)) as pulmonary_veins_normal,
    cast(pulmonary_veins_papvc as varchar(255)) as pulmonary_veins_papvc,
    cast(pulmonary_veins_tapvc as varchar(255)) as pulmonary_veins_tapvc,
    cast(situs_cardiotype_undetermined as varchar(255)) as situs_cardiotype_undetermined,
    cast(situs_atrial as varchar(255)) as situs_atrial,
    cast(situs_ventricular_loop as varchar(255)) as situs_ventricular_loop,
    cast(situs_great_artery_relationships as varchar(255)) as situs_great_artery_relationships,
    cast(situs_cardiac_position as varchar(255)) as situs_cardiac_position,
    cast(situs_visceral_situs as varchar(255)) as situs_visceral_situs,
    cast(systemic_veins_coronary_sinus as varchar(255)) as systemic_veins_coronary_sinus,
    cast(systemic_veins_inferior_vena_cava as varchar(255)) as systemic_veins_inferior_vena_cava,
    cast(systemic_veins_left_superior_vena_cava as varchar(255)) as systemic_veins_left_superior_vena_cava,
    cast(systemic_veins_right_superior_vena_cava as varchar(255)) as systemic_veins_right_superior_vena_cava
from sq_echo_fetal_study_veins
