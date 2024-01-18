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
    where lower(syngo_echo_observationname.name) in ('lv_structure_severity_0_obs', 'vsd_-_not_obs', 'vsd_1_location_size_chop_obs', 'chw_rv_diastolic_function_obs', 'chw_rv_systolic_function_obs',
'rv_structure_severity_obs', 'nl_rv_size_and_fx_obs', 'chw_lv_systolic_function_obs', 'nl_lv_size_and_fx_obs')
),


observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'lv_structure_severity_0_obs' then displayvalue end as lv_structure_severity_0_obs,
        case when observation_name = 'vsd_-_not_obs' then displayvalue end as vsd_not_obs,
        case when observation_name = 'vsd_1_location_size_chop_obs' then displayvalue end as vsd_1_location_size_chop_obs,
        case when observation_name = 'chw_rv_diastolic_function_obs' then displayvalue end as chw_rv_diastolic_function_obs,
        case when observation_name = 'chw_rv_systolic_function_obs' then displayvalue end as chw_rv_systolic_function_obs,
        case when observation_name = 'rv_structure_severity_obs' then displayvalue end as rv_structure_severity_obs,
        case when observation_name = 'nl_rv_size_and_fx_obs' then displayvalue end as nl_rv_size_and_fx_obs,
        case when observation_name = 'chw_lv_systolic_function_obs' then displayvalue end as chw_lv_systolic_function_obs,
        case when observation_name = 'nl_lv_size_and_fx_obs' then displayvalue end as nl_lv_size_and_fx_obs
    from observation_display_values
),


observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(lv_structure_severity_0_obs, ';') as lv_structure_severity_0_obs,
        group_concat(vsd_not_obs, ';') as vsd_not_obs,
        group_concat(vsd_1_location_size_chop_obs, ';') as vsd_1_location_size_chop_obs,
        group_concat(chw_rv_diastolic_function_obs, ';') as chw_rv_diastolic_function_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(chw_rv_systolic_function_obs, ';') as chw_rv_systolic_function_obs,
        group_concat(rv_structure_severity_obs, ';') as rv_structure_severity_obs,
        group_concat(nl_rv_size_and_fx_obs, ';') as nl_rv_size_and_fx_obs,
        group_concat(chw_lv_systolic_function_obs, ';') as chw_lv_systolic_function_obs
    from observations
    group by studyid, ownerid
),

observation_group_3 as (
    select
        studyid,
        ownerid,
        group_concat(nl_lv_size_and_fx_obs, ';') as nl_lv_size_and_fx_obs
    from observations
    group by studyid, ownerid
),

sq_echo_fetal_study_ventricles as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        nl_rv_size_and_fx_obs as right_ventricle_size_fx,
        chw_rv_diastolic_function_obs as right_ventricle_diastolic_function,
        rv_structure_severity_obs as right_ventricle_structure_severity,
        chw_rv_systolic_function_obs as right_ventricle_systolic_function,
        nl_lv_size_and_fx_obs as left_ventricle_size_fx,
        lv_structure_severity_0_obs as left_ventricle_structure_severity,
        chw_lv_systolic_function_obs as left_ventricle_systolic_function,
        vsd_not_obs as vsd_not_observed,
        vsd_1_location_size_chop_obs as vsd_type_size
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
and (lv_structure_severity_0_obs is not null or nl_lv_size_and_fx_obs is not null or nl_rv_size_and_fx_obs is not null or rv_structure_severity_obs is not null or vsd_not_obs is not null or vsd_1_location_size_chop_obs is not null or chw_lv_systolic_function_obs is not null or chw_rv_diastolic_function_obs is not null or chw_rv_systolic_function_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(right_ventricle_size_fx as varchar(255)) as right_ventricle_size_fx,
    cast(right_ventricle_diastolic_function as varchar(255)) as right_ventricle_diastolic_function,
    cast(right_ventricle_structure_severity as varchar(255)) as right_ventricle_structure_severity,
    cast(right_ventricle_systolic_function as varchar(255)) as right_ventricle_systolic_function,
    cast(left_ventricle_size_fx as varchar(255)) as left_ventricle_size_fx,
    cast(left_ventricle_structure_severity as varchar(255)) as left_ventricle_structure_severity,
    cast(left_ventricle_systolic_function as varchar(255)) as left_ventricle_systolic_function,
    cast(vsd_not_observed as varchar(255)) as vsd_not_observed,
    cast(vsd_type_size as varchar(255)) as vsd_type_size
from sq_echo_fetal_study_ventricles
