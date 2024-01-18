with echos as (
    select
        source_system_id,
        echo_study_id
    from {{ ref('echo_study') }}
    where lower(source_system) = 'syngo'
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
    where lower(syngo_echo_observationname.name) in ('rvot_obst_severity_type_obs', 'chw_rvot_no_obst_obs', 'pab_pv_regurgitation_degree_obs',
        'chw_pv_stenosis_degree_doppler_obs', 'conduit_stenosis_obs', 'conduit_regurg_obs', 'chw_pv_normal_outflow_obs',
        'neo_pulmonary_valve_regurg_obs', 'neo_pulmonary_obstruction_obs', 'lvot_obstruction_type_severity_obs',
        'chw_lvot_no_obst_summary_obs', 'chw_aov_normal_outflow_obs', 'av_supravalvar_stenosis_type_severity_obs',
        'pab_aov_regurgitation_degree_obs', 'chw_aov_stenosis_degree_doppler_obs', 'neo_aortic_valve_regurg_obs',
        'neo_aortic_obstruction_obs', 'neo_aortic_valve_grad_obs', 'ao_root_obs', 'av_anatomy_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'rvot_obst_severity_type_obs' then displayvalue end as rvot_obstruction_severity_type,
        case when observation_name = 'chw_rvot_no_obst_obs' then displayvalue end as rvot_obstruction,
        case when observation_name = 'pab_pv_regurgitation_degree_obs' then displayvalue end as pulmonary_regurgitation,
        case when observation_name = 'chw_pv_stenosis_degree_doppler_obs' then displayvalue end as pulmonary_stenosis,
        case when observation_name = 'conduit_stenosis_obs' then displayvalue end as pulmonary_conduit_stenosis,
        case when observation_name = 'conduit_regurg_obs' then displayvalue end as pulmonary_conduit_regurgitation,
        case when observation_name = 'chw_pv_normal_outflow_obs' then displayvalue end as pulmonary_normal_doppler,
        case when observation_name = 'neo_pulmonary_valve_regurg_obs' then displayvalue end as neo_pulmonary_regurgitation,
        case when observation_name = 'neo_pulmonary_obstruction_obs' then displayvalue end as neo_pulmonary_obstruction,
        case when observation_name = 'lvot_obstruction_type_severity_obs' then displayvalue end as lvot_obstruction_severity_type,
        case when observation_name = 'chw_lvot_no_obst_summary_obs' then displayvalue end as lvot_obstruction,
        case when observation_name = 'chw_aov_normal_outflow_obs' then displayvalue end as aortic_valve_normal_doppler,
        case when observation_name = 'av_supravalvar_stenosis_type_severity_obs' then displayvalue end as aortic_valve_supravalvlar_stenosis,
        case when observation_name = 'pab_aov_regurgitation_degree_obs' then displayvalue end as aortic_valve_regurgitation,
        case when observation_name = 'chw_aov_stenosis_degree_doppler_obs' then displayvalue end as aortic_valve_stenosis,
        case when observation_name = 'neo_aortic_valve_regurg_obs' then displayvalue end as aortic_valve_neoaortic_valve_regurgitation,
        case when observation_name = 'neo_aortic_obstruction_obs' then displayvalue end as aortic_valve_neoaortic_obstruction,
        case when observation_name = 'neo_aortic_valve_grad_obs' then displayvalue end as aortic_valve_neo_aov_gradient,
        case when observation_name = 'ao_root_obs' then displayvalue end as aortic_valve_ao_root,
        case when observation_name = 'av_anatomy_obs' then displayvalue end as aortic_valve_anatomy
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        group_concat(rvot_obstruction_severity_type, ';') as rvot_obstruction_severity_type,
        group_concat(rvot_obstruction, ';') as rvot_obstruction,
        group_concat(pulmonary_regurgitation, ';') as pulmonary_regurgitation,
        group_concat(pulmonary_stenosis, ';') as pulmonary_stenosis
    from observations
    group by studyid
),

observation_group_2 as (
    select
        studyid,
        group_concat(pulmonary_conduit_stenosis, ';') as pulmonary_conduit_stenosis,
        group_concat(pulmonary_conduit_regurgitation, ';') as pulmonary_conduit_regurgitation,
        group_concat(pulmonary_normal_doppler, ';') as pulmonary_normal_doppler,
        group_concat(neo_pulmonary_regurgitation, ';') as neo_pulmonary_regurgitation
    from observations
    group by studyid
),

observation_group_3 as (
    select
        studyid,
        group_concat(neo_pulmonary_obstruction, ';') as neo_pulmonary_obstruction,
        group_concat(lvot_obstruction_severity_type, ';') as lvot_obstruction_severity_type,
        group_concat(lvot_obstruction, ';') as lvot_obstruction,
        group_concat(aortic_valve_normal_doppler, ';') as aortic_valve_normal_doppler
    from observations
    group by studyid
),
observation_group_4 as (
    select
        studyid,
        group_concat(aortic_valve_supravalvlar_stenosis, ';') as aortic_valve_supravalvlar_stenosis,
        group_concat(aortic_valve_regurgitation, ';') as aortic_valve_regurgitation,
        group_concat(aortic_valve_stenosis, ';') as aortic_valve_stenosis,
        group_concat(aortic_valve_neoaortic_valve_regurgitation, ';') as aortic_valve_neoaortic_valve_regurgitation
    from observations
    group by studyid
),
observation_group_5 as (
    select
        studyid,
        group_concat(aortic_valve_neoaortic_obstruction, ';') as aortic_valve_neoaortic_obstruction,
        group_concat(aortic_valve_neo_aov_gradient, ';') as aortic_valve_neo_aov_gradient,
        group_concat(aortic_valve_ao_root, ';') as aortic_valve_ao_root,
        group_concat(aortic_valve_anatomy, ';') as aortic_valve_anatomy
    from observations
    group by studyid
),

sq_outlets as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(rvot_obstruction_severity_type as varchar(255)) as rvot_obstruction_severity_type,
        cast(rvot_obstruction as varchar(255)) as rvot_obstruction,
        cast(pulmonary_regurgitation as varchar(255)) as pulmonary_regurgitation,
        cast(pulmonary_stenosis as varchar(255)) as pulmonary_stenosis,
        cast(pulmonary_conduit_stenosis as varchar(255)) as pulmonary_conduit_stenosis,
        cast(pulmonary_conduit_regurgitation as varchar(255)) as pulmonary_conduit_regurgitation,
        cast(pulmonary_normal_doppler as varchar(255)) as pulmonary_normal_doppler,
        cast(neo_pulmonary_regurgitation as varchar(255)) as neo_pulmonary_regurgitation,
        cast(neo_pulmonary_obstruction as varchar(255)) as neo_pulmonary_obstruction,
        cast(lvot_obstruction_severity_type as varchar(255)) as lvot_obstruction_severity_type,
        cast(lvot_obstruction as varchar(255)) as lvot_obstruction,
        cast(aortic_valve_normal_doppler as varchar(255)) as aortic_valve_normal_doppler,
        cast(aortic_valve_supravalvlar_stenosis as varchar(255)) as aortic_valve_supravalvlar_stenosis,
        cast(aortic_valve_regurgitation as varchar(255)) as aortic_valve_regurgitation,
        cast(aortic_valve_stenosis as varchar(255)) as aortic_valve_stenosis,
        cast(aortic_valve_neoaortic_valve_regurgitation as varchar(255)) as aortic_valve_neoaortic_valve_regurgitation,
        cast(aortic_valve_neoaortic_obstruction as varchar(255)) as aortic_valve_neoaortic_obstruction,
        cast(aortic_valve_neo_aov_gradient as varchar(255)) as aortic_valve_neo_aov_gradient,
        cast(aortic_valve_ao_root as varchar(255)) as aortic_valve_ao_root,
        cast(aortic_valve_anatomy as varchar(255)) as aortic_valve_anatomy
    from echos
    left join observation_group_1
        on echos.source_system_id = observation_group_1.studyid
    left join observation_group_2
        on echos.source_system_id = observation_group_2.studyid
    left join observation_group_3
        on echos.source_system_id = observation_group_3.studyid
    left join observation_group_4
        on echos.source_system_id = observation_group_4.studyid
    left join observation_group_5
        on echos.source_system_id = observation_group_5.studyid
    where (rvot_obstruction_severity_type is not null or rvot_obstruction is not null or pulmonary_regurgitation is not null
        or pulmonary_stenosis is not null or pulmonary_conduit_stenosis is not null or pulmonary_conduit_regurgitation is not null
        or pulmonary_normal_doppler is not null or neo_pulmonary_regurgitation is not null or neo_pulmonary_obstruction is not null
        or lvot_obstruction_severity_type is not null or lvot_obstruction is not null or aortic_valve_normal_doppler is not null
        or aortic_valve_supravalvlar_stenosis is not null or aortic_valve_regurgitation is not null
        or aortic_valve_stenosis is not null or aortic_valve_neoaortic_valve_regurgitation is not null
        or aortic_valve_neoaortic_obstruction is not null or aortic_valve_neo_aov_gradient is not null
        or aortic_valve_ao_root is not null or aortic_valve_anatomy is not null)

)

select
    echo_study_id,
    rvot_obstruction_severity_type,
    rvot_obstruction,
    pulmonary_regurgitation,
    pulmonary_stenosis,
    pulmonary_conduit_stenosis,
    pulmonary_conduit_regurgitation,
    pulmonary_normal_doppler,
    neo_pulmonary_regurgitation,
    neo_pulmonary_obstruction,
    lvot_obstruction_severity_type,
    lvot_obstruction,
    aortic_valve_normal_doppler,
    aortic_valve_supravalvlar_stenosis,
    aortic_valve_regurgitation,
    aortic_valve_stenosis,
    aortic_valve_neoaortic_valve_regurgitation,
    aortic_valve_neoaortic_obstruction,
    aortic_valve_neo_aov_gradient,
    aortic_valve_ao_root,
    aortic_valve_anatomy
from sq_outlets
