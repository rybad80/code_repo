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
        cast(avg(case when lower(name) = 'pda_ampulla_orifice_diam_calc' then floatvalue end) as decimal (28, 15)) as pda_ampulla_orifince_dimen_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('pda_ampulla_orifice_diam_calc')
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
    where lower(syngo_echo_observationname.name) in ('chw_ao_flow_characteristics_obs', 'chw_ao_ascending_size_obs',
        'coarctation_type_severity_chop_obs', 'chw_pa_branches_normal_summary_obs', 'lpa_dx_structure_severity_obs',
        'rpa_dx_structure_severity_obs', 'pa_supravalvular_stenosis_severity_obs', 'ca_not_eval_obs', 'ca_nwv_obs',
        'cas_normal_obs', 'pda_type_size_chop_obs', 'pda_shunt_direction_chop_obs', 'pda_flow_restriction_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'chw_ao_flow_characteristics_obs' then displayvalue end as aorta_flow,
        case when observation_name = 'chw_ao_ascending_size_obs' then displayvalue end as aorta_ascending_size,
        case when observation_name = 'coarctation_type_severity_chop_obs' then displayvalue end as aorta_coarct_type_severity,
        case when observation_name = 'chw_pa_branches_normal_summary_obs' then displayvalue end as pulmonary_arteries_branches_normal,
        case when observation_name = 'lpa_dx_structure_severity_obs' then displayvalue end as pulmonary_arteries_lpa_structure_severity,
        case when observation_name = 'rpa_dx_structure_severity_obs' then displayvalue end as pulmonary_arteries_rpa_structure_severity,
        case when observation_name = 'pa_supravalvular_stenosis_severity_obs' then displayvalue end as pulmonary_arteries_supravalvular_stenosis,
        case when observation_name = 'ca_not_eval_obs' then displayvalue end as coronary_arteries_not_eval,
        case when observation_name = 'ca_nwv_obs' then displayvalue end as coronary_arteries_nmv,
        case when observation_name = 'cas_normal_obs' then displayvalue end as coronary_arteries_normal,
        case when observation_name = 'pda_type_size_chop_obs' then displayvalue end as pda_type_size,
        case when observation_name = 'pda_shunt_direction_chop_obs' then displayvalue end as pda_shunt_direction,
        case when observation_name = 'pda_flow_restriction_obs' then displayvalue end as pda_flow_restriction
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        group_concat(aorta_flow, ';') as aorta_flow,
        group_concat(aorta_ascending_size, ';') as aorta_ascending_size,
        group_concat(aorta_coarct_type_severity, ';') as aorta_coarct_type_severity,
        group_concat(pulmonary_arteries_branches_normal, ';') as pulmonary_arteries_branches_normal
    from observations
    group by studyid
),

observation_group_2 as (
    select
        studyid,
        group_concat(pulmonary_arteries_lpa_structure_severity, ';') as pulmonary_arteries_lpa_structure_severity,
        group_concat(pulmonary_arteries_rpa_structure_severity, ';') as pulmonary_arteries_rpa_structure_severity,
        group_concat(pulmonary_arteries_supravalvular_stenosis, ';') as pulmonary_arteries_supravalvular_stenosis,
        group_concat(coronary_arteries_not_eval, ';') as coronary_arteries_not_eval
    from observations
    group by studyid
),

observation_group_3 as (
    select
        studyid,
        group_concat(coronary_arteries_nmv, ';') as coronary_arteries_nmv,
        group_concat(coronary_arteries_normal, ';') as coronary_arteries_normal,
        group_concat(pda_type_size, ';') as pda_type_size,
        group_concat(pda_shunt_direction, ';') as pda_shunt_direction,
        group_concat(pda_flow_restriction, ';') as pda_flow_restriction
    from observations
    group by studyid
),

sq_echo_study_arteries as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(aorta_flow as varchar(255)) as aorta_flow,
        cast(aorta_ascending_size as varchar(255)) as aorta_ascending_size,
        cast(aorta_coarct_type_severity as varchar(255)) as aorta_coarct_type_severity,
        cast(pulmonary_arteries_branches_normal as varchar(255)) as pulmonary_arteries_branches_normal,
        cast(pulmonary_arteries_lpa_structure_severity as varchar(255)) as pulmonary_arteries_lpa_structure_severity,
        cast(pulmonary_arteries_rpa_structure_severity as varchar(255)) as pulmonary_arteries_rpa_structure_severity,
        cast(pulmonary_arteries_supravalvular_stenosis as varchar(255)) as pulmonary_arteries_supravalvular_stenosis,
        cast(coronary_arteries_not_eval as varchar(255)) as coronary_arteries_not_eval,
        cast(coronary_arteries_nmv as varchar(255)) as coronary_arteries_nmv,
        cast(coronary_arteries_normal as varchar(255)) as coronary_arteries_normal,
        cast(pda_type_size as varchar(255)) as pda_type_size,
        cast(pda_shunt_direction as varchar(255)) as pda_shunt_direction,
        cast(pda_ampulla_orifince_dimen_avg as numeric(28, 15)) as pda_ampulla_orifince_dimen_avg,
        cast(pda_flow_restriction as varchar(255)) as pda_flow_restriction
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    left join observation_group_1
        on echos.source_system_id = observation_group_1.studyid
    left join observation_group_2
        on echos.source_system_id = observation_group_2.studyid
    left join observation_group_3
        on echos.source_system_id = observation_group_3.studyid
    where (aorta_flow is not null or aorta_ascending_size is not null or aorta_coarct_type_severity is not null
        or pulmonary_arteries_branches_normal is not null or pulmonary_arteries_lpa_structure_severity is not null
        or pulmonary_arteries_rpa_structure_severity is not null or pulmonary_arteries_supravalvular_stenosis is not null
        or coronary_arteries_not_eval is not null or coronary_arteries_nmv is not null
        or coronary_arteries_normal is not null or pda_type_size is not null or pda_shunt_direction is not null
        or pda_ampulla_orifince_dimen_avg is not null or pda_flow_restriction is not null)
)

select
    echo_study_id,
    aorta_flow,
    aorta_ascending_size,
    aorta_coarct_type_severity,
    pulmonary_arteries_branches_normal,
    pulmonary_arteries_lpa_structure_severity,
    pulmonary_arteries_rpa_structure_severity,
    pulmonary_arteries_supravalvular_stenosis,
    coronary_arteries_not_eval,
    coronary_arteries_nmv,
    coronary_arteries_normal,
    pda_type_size,
    pda_shunt_direction,
    pda_ampulla_orifince_dimen_avg,
    pda_flow_restriction
from sq_echo_study_arteries
