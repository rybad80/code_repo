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
    where lower(syngo_echo_observationname.name) in ('chw_pv_atresia_obs', 'av_anatomy_obs', 'chw_aov_stenosis_degree_doppler_obs',
    'nml_aortic_valve_obs', 'chw_rvot_no_obst_obs', 'pab_aov_regurgitation_degree_obs', 'aortic_root_dilatation_severity_obs',
    'chw_pv_stenosis_degree_doppler_obs', 'rvot_obst_severity_type_obs', 'chw_lvot_no_obst_summary_obs', 'pab_pv_regurgitation_degree_obs',
    'lvot_obstruction_type_severity_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'chw_pv_atresia_obs' then displayvalue end as chw_pv_atresia_obs,
        case when observation_name = 'av_anatomy_obs' then displayvalue end as av_anatomy_obs,
        case when observation_name = 'chw_aov_stenosis_degree_doppler_obs' then displayvalue end as chw_aov_stenosis_degree_doppler_obs,
        case when observation_name = 'nml_aortic_valve_obs' then displayvalue end as nml_aortic_valve_obs,
        case when observation_name = 'chw_rvot_no_obst_obs' then displayvalue end as chw_rvot_no_obst_obs,
        case when observation_name = 'pab_aov_regurgitation_degree_obs' then displayvalue end as pab_aov_regurgitation_degree_obs,
        case when observation_name = 'aortic_root_dilatation_severity_obs' then displayvalue end as aortic_root_dilatation_severity_obs,
        case when observation_name = 'chw_pv_stenosis_degree_doppler_obs' then displayvalue end as chw_pv_stenosis_degree_doppler_obs,
        case when observation_name = 'rvot_obst_severity_type_obs' then displayvalue end as rvot_obst_severity_type_obs,
        case when observation_name = 'chw_lvot_no_obst_summary_obs' then displayvalue end as chw_lvot_no_obst_summary_obs,
        case when observation_name = 'pab_pv_regurgitation_degree_obs' then displayvalue end as pab_pv_regurgitation_degree_obs,
case when observation_name = 'lvot_obstruction_type_severity_obs' then displayvalue end as lvot_obstruction_type_severity_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(chw_pv_atresia_obs, ';') as chw_pv_atresia_obs,
        group_concat(av_anatomy_obs, ';') as av_anatomy_obs,
        group_concat(chw_aov_stenosis_degree_doppler_obs, ';') as chw_aov_stenosis_degree_doppler_obs,
        group_concat(nml_aortic_valve_obs, ';') as nml_aortic_valve_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(chw_rvot_no_obst_obs, ';') as chw_rvot_no_obst_obs,
        group_concat(pab_aov_regurgitation_degree_obs, ';') as pab_aov_regurgitation_degree_obs,
        group_concat(aortic_root_dilatation_severity_obs, ';') as aortic_root_dilatation_severity_obs,
        group_concat(chw_pv_stenosis_degree_doppler_obs, ';') as chw_pv_stenosis_degree_doppler_obs
    from observations
    group by studyid, ownerid
),

observation_group_3 as (
    select
        studyid,
        ownerid,
        group_concat(rvot_obst_severity_type_obs, ';') as rvot_obst_severity_type_obs,
        group_concat(chw_lvot_no_obst_summary_obs, ';') as chw_lvot_no_obst_summary_obs,
        group_concat(pab_pv_regurgitation_degree_obs, ';') as pab_pv_regurgitation_degree_obs,
        group_concat(lvot_obstruction_type_severity_obs, ';') as lvot_obstruction_type_severity_obs
    from observations
    group by studyid, ownerid
),

sq_echo_fetal_study_outlets as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        chw_rvot_no_obst_obs as rvot_obstruction,
        rvot_obst_severity_type_obs as rvot_obstruction_severity_type,
        chw_pv_atresia_obs as pulmonary_atresia,
        pab_pv_regurgitation_degree_obs as pulmonary_regurgitation,
        chw_pv_stenosis_degree_doppler_obs as pulmonary_stenosis,
        chw_lvot_no_obst_summary_obs as lvot_obstruction,
        lvot_obstruction_type_severity_obs as lvot_obstruction_severity_type,
        nml_aortic_valve_obs as aortic_valve_normal,
        av_anatomy_obs as aortic_valve_anatomy,
        pab_aov_regurgitation_degree_obs as aortic_valve_regurgitation,
        chw_aov_stenosis_degree_doppler_obs as aortic_valve_stenosis,
        aortic_root_dilatation_severity_obs as aortic_valve_root_diliation
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
where ownertype in (1, 2)
    and (av_anatomy_obs is not null or aortic_root_dilatation_severity_obs is not null or lvot_obstruction_type_severity_obs is not null or nml_aortic_valve_obs is not null or pab_aov_regurgitation_degree_obs is not null or pab_pv_regurgitation_degree_obs is not null or rvot_obst_severity_type_obs is not null or chw_aov_stenosis_degree_doppler_obs is not null or chw_lvot_no_obst_summary_obs is not null or chw_pv_atresia_obs is not null or chw_pv_stenosis_degree_doppler_obs is not null or chw_rvot_no_obst_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(rvot_obstruction as varchar(255)) as rvot_obstruction,
    cast(rvot_obstruction_severity_type as varchar(255)) as rvot_obstruction_severity_type,
    cast(pulmonary_atresia as varchar(255)) as pulmonary_atresia,
    cast(pulmonary_regurgitation as varchar(255)) as pulmonary_regurgitation,
    cast(pulmonary_stenosis as varchar(255)) as pulmonary_stenosis,
    cast(lvot_obstruction as varchar(255)) as lvot_obstruction,
    cast(lvot_obstruction_severity_type as varchar(255)) as lvot_obstruction_severity_type,
    cast(aortic_valve_normal as varchar(255)) as aortic_valve_normal,
    cast(aortic_valve_anatomy as varchar(255)) as aortic_valve_anatomy,
    cast(aortic_valve_regurgitation as varchar(255)) as aortic_valve_regurgitation,
    cast(aortic_valve_stenosis as varchar(255)) as aortic_valve_stenosis,
    cast(aortic_valve_root_diliation as varchar(255)) as aortic_valve_root_diliation
from sq_echo_fetal_study_outlets
