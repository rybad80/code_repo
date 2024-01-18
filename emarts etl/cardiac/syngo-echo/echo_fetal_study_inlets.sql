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
    where lower(syngo_echo_observationname.name) in ('avc_type_obs', 'chw_mv_stenosis_1_obs',
    'mv_structure_and_severity_obs', 'chw_tv_ebstein_s_anomaly_obs', 'tv_atresia_obs', 'pab_tv_regurgitation_degree_obs',
    'chw_tv_normal_inflow_obs', 'common_avv_regurg_obs', 'chw_common_ventricle_morphology_obs',
    'tv_structure_and_severity_obs', 'pab_mv_regurgitation_degree_obs')
),
observations as (
    select
        studyid,
        ownerid,
case when observation_name = 'avc_type_obs' then displayvalue end as avc_type_obs,
case when observation_name = 'chw_mv_stenosis_1_obs' then displayvalue end as chw_mv_stenosis_1_obs,
case when observation_name = 'mv_structure_and_severity_obs' then displayvalue end as mv_structure_and_severity_obs,
case when observation_name = 'chw_tv_ebstein_s_anomaly_obs' then displayvalue end as chw_tv_ebstein_s_anomaly_obs,
case when observation_name = 'tv_atresia_obs' then displayvalue end as tv_atresia_obs,
case when observation_name = 'pab_tv_regurgitation_degree_obs' then displayvalue end as pab_tv_regurgitation_degree_obs,
case when observation_name = 'chw_tv_normal_inflow_obs' then displayvalue end as chw_tv_normal_inflow_obs,
case when observation_name = 'common_avv_regurg_obs' then displayvalue end as common_avv_regurg_obs,
case when observation_name = 'chw_common_ventricle_morphology_obs' then displayvalue end as chw_common_ventricle_morphology_obs,
case when observation_name = 'tv_structure_and_severity_obs' then displayvalue end as tv_structure_and_severity_obs,
case when observation_name = 'pab_mv_regurgitation_degree_obs' then displayvalue end as pab_mv_regurgitation_degree_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(avc_type_obs, ';') as avc_type_obs,
        group_concat(chw_mv_stenosis_1_obs, ';') as chw_mv_stenosis_1_obs,
        group_concat(mv_structure_and_severity_obs, ';') as mv_structure_and_severity_obs,
        group_concat(chw_tv_ebstein_s_anomaly_obs, ';') as chw_tv_ebstein_s_anomaly_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(tv_atresia_obs, ';') as tv_atresia_obs,
        group_concat(pab_tv_regurgitation_degree_obs, ';') as pab_tv_regurgitation_degree_obs,
        group_concat(chw_tv_normal_inflow_obs, ';') as chw_tv_normal_inflow_obs,
        group_concat(common_avv_regurg_obs, ';') as common_avv_regurg_obs
    from observations
    group by studyid, ownerid
),

observation_group_3 as (
    select
        studyid,
        ownerid,
        group_concat(chw_common_ventricle_morphology_obs, ';') as chw_common_ventricle_morphology_obs,
        group_concat(tv_structure_and_severity_obs, ';') as tv_structure_and_severity_obs,
        group_concat(pab_mv_regurgitation_degree_obs, ';') as pab_mv_regurgitation_degree_obs
    from observations
    group by studyid, ownerid
),

sq_echo_fetal_study_inlets as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        chw_tv_ebstein_s_anomaly_obs as tricuspid_ebsteins_anomaly,
        chw_tv_normal_inflow_obs as tricuspid_normal_doppler,
        tv_atresia_obs as tricuspid_atresia,
        tv_structure_and_severity_obs as tricuspid_structure_severity,
        pab_tv_regurgitation_degree_obs as tricuspid_regurgitation,
        mv_structure_and_severity_obs as mitral_structure_severity,
        pab_mv_regurgitation_degree_obs as mitral_regurgitation,
        chw_mv_stenosis_1_obs as mitral_stenosis,
        avc_type_obs as avcanal_type,
        common_avv_regurg_obs as avcanal_common_avv_regurgitation,
        chw_common_ventricle_morphology_obs as common_single_ventricle_morphology
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
        and (avc_type_obs is not null or common_avv_regurg_obs is not null or mv_structure_and_severity_obs is not null or pab_mv_regurgitation_degree_obs is not null or pab_tv_regurgitation_degree_obs is not null or tv_atresia_obs is not null or tv_structure_and_severity_obs is not null or chw_mv_stenosis_1_obs is not null or chw_tv_ebstein_s_anomaly_obs is not null or chw_tv_normal_inflow_obs is not null or chw_common_ventricle_morphology_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(tricuspid_ebsteins_anomaly as varchar(255)) as tricuspid_ebsteins_anomaly,
    cast(tricuspid_normal_doppler as varchar(255)) as tricuspid_normal_doppler,
    cast(tricuspid_atresia as varchar(255)) as tricuspid_atresia,
    cast(tricuspid_structure_severity as varchar(255)) as tricuspid_structure_severity,
    cast(tricuspid_regurgitation as varchar(255)) as tricuspid_regurgitation,
    cast(mitral_structure_severity as varchar(255)) as mitral_structure_severity,
    cast(mitral_regurgitation as varchar(255)) as mitral_regurgitation,
    cast(mitral_stenosis as varchar(255)) as mitral_stenosis,
    cast(avcanal_type as varchar(255)) as avcanal_type,
    cast(avcanal_common_avv_regurgitation as varchar(255)) as avcanal_common_avv_regurgitation,
    cast(common_single_ventricle_morphology as varchar(255)) as common_single_ventricle_morphology
from sq_echo_fetal_study_inlets
