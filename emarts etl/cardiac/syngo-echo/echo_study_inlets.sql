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
        cast(avg(case when lower(name) = 'mv_mean_grad_calc' then floatvalue end) as decimal (28, 15)) as mitral_mean_gradient_avg,
        cast(avg(case when lower(name) = 'tv_mean_grad_calc' then floatvalue end) as decimal (28, 15)) as tricuspid_mean_gradient_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('mv_mean_grad_calc', 'tv_mean_grad_calc')
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
    where lower(syngo_echo_observationname.name) in ('pab_tv_regurgitation_degree_obs', 'chw_tv_stenosis_degree_obs', 'chw_tv_ebstein_s_anomaly_obs',
        'pab_mv_regurgitation_degree_obs', 'chw_mv_stenosis_1_obs', 'avc_type_obs', 'rastelli_type_1_obs', 'common_avv_regurg_obs',
        'avsd_left_avvr_obs', 'avsd_rt_avvr_obs', 'chw_common_ventricle_av_valve_stenosis_degree_obs',
        'avsd_balance_obs', 'chw_common_ventricle_av_valve_regurg_degree_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'pab_tv_regurgitation_degree_obs' then displayvalue end as tricuspid_regurgitation,
        case when observation_name = 'chw_tv_stenosis_degree_obs' then displayvalue end as tricuspid_stenosis,
        case when observation_name = 'chw_tv_ebstein_s_anomaly_obs' then displayvalue end as tricuspid_ebsteins_anomaly,
        case when observation_name = 'pab_mv_regurgitation_degree_obs' then displayvalue end as mitral_regurgitation,
        case when observation_name = 'chw_mv_stenosis_1_obs' then displayvalue end as mitral_stenosis,
        case when observation_name = 'avc_type_obs' then displayvalue end as avcanal_type,
        case when observation_name = 'rastelli_type_1_obs' then displayvalue end as avcanal_rastelli_type,
        case when observation_name = 'common_avv_regurg_obs' then displayvalue end as avcanal_common_avv_regurgitation,
        case when observation_name = 'avsd_left_avvr_obs' then displayvalue end as avcanal_left_avv_regurgitation,
        case when observation_name = 'avsd_rt_avvr_obs' then displayvalue end as avcanal_right_avv_regurgitation,
        case when observation_name = 'chw_common_ventricle_av_valve_stenosis_degree_obs' then displayvalue end as avcanal_stenosis,
        case when observation_name = 'avsd_balance_obs' then displayvalue end as avcanal_unbalanced,
        case when observation_name = 'chw_common_ventricle_av_valve_regurg_degree_obs' then displayvalue end as common_single_ventricle_av_valve_regurgitation
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        group_concat(tricuspid_regurgitation, ';') as tricuspid_regurgitation,
        group_concat(tricuspid_stenosis, ';') as tricuspid_stenosis,
        group_concat(tricuspid_ebsteins_anomaly, ';') as tricuspid_ebsteins_anomaly,
        group_concat(mitral_regurgitation, ';') as mitral_regurgitation
    from observations
    group by studyid
),

observation_group_2 as (
    select
        studyid,
        group_concat(mitral_stenosis, ';') as mitral_stenosis,
        group_concat(avcanal_type, ';') as avcanal_type,
        group_concat(avcanal_rastelli_type, ';') as avcanal_rastelli_type,
        group_concat(avcanal_common_avv_regurgitation, ';') as avcanal_common_avv_regurgitation
    from observations
    group by studyid
),

observation_group_3 as (
    select
        studyid,
        group_concat(avcanal_left_avv_regurgitation, ';') as avcanal_left_avv_regurgitation,
        group_concat(avcanal_right_avv_regurgitation, ';') as avcanal_right_avv_regurgitation,
        group_concat(avcanal_stenosis, ';') as avcanal_stenosis,
        group_concat(avcanal_unbalanced, ';') as avcanal_unbalanced,
        group_concat(common_single_ventricle_av_valve_regurgitation, ';') as common_single_ventricle_av_valve_regurgitation
    from observations
    group by studyid
),



sq_echo_study_inlets as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(tricuspid_regurgitation as varchar(255)) as tricuspid_regurgitation,
        cast(tricuspid_stenosis as varchar(255)) as tricuspid_stenosis,
        cast(tricuspid_mean_gradient_avg as numeric(28, 15)) as tricuspid_mean_gradient_avg,
        cast(tricuspid_ebsteins_anomaly as varchar(255)) as tricuspid_ebsteins_anomaly,
        cast(mitral_regurgitation as varchar(255)) as mitral_regurgitation,
        cast(mitral_stenosis as varchar(255)) as mitral_stenosis,
        cast(mitral_mean_gradient_avg as numeric(28, 15)) as mitral_mean_gradient_avg,
        cast(avcanal_type as varchar(255)) as avcanal_type,
        cast(avcanal_rastelli_type as varchar(255)) as avcanal_rastelli_type,
        cast(avcanal_common_avv_regurgitation as varchar(255)) as avcanal_common_avv_regurgitation,
        cast(avcanal_left_avv_regurgitation as varchar(255)) as avcanal_left_avv_regurgitation,
        cast(avcanal_right_avv_regurgitation as varchar(255)) as avcanal_right_avv_regurgitation,
        cast(avcanal_stenosis as varchar(255)) as avcanal_stenosis,
        cast(avcanal_unbalanced as varchar(255)) as avcanal_unbalanced,
        cast(common_single_ventricle_av_valve_regurgitation as varchar(255)) as common_single_ventricle_av_valve_regurgitation
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    left join observation_group_1
        on echos.source_system_id = observation_group_1.studyid
    left join observation_group_2
        on echos.source_system_id = observation_group_2.studyid
    left join observation_group_3
        on echos.source_system_id = observation_group_3.studyid
    where
        (tricuspid_regurgitation is not null or tricuspid_stenosis is not null or tricuspid_mean_gradient_avg is not null
            or tricuspid_ebsteins_anomaly is not null or mitral_regurgitation is not null or mitral_stenosis is not null
            or mitral_mean_gradient_avg is not null or avcanal_type is not null or avcanal_rastelli_type is not null
            or avcanal_common_avv_regurgitation is not null or avcanal_left_avv_regurgitation is not null
            or avcanal_right_avv_regurgitation is not null or avcanal_stenosis is not null or avcanal_unbalanced is not null
            or common_single_ventricle_av_valve_regurgitation is not null)
)

select
    echo_study_id,
    tricuspid_regurgitation,
    tricuspid_stenosis,
    tricuspid_mean_gradient_avg,
    tricuspid_ebsteins_anomaly,
    mitral_regurgitation,
    mitral_stenosis,
    mitral_mean_gradient_avg,
    avcanal_type,
    avcanal_rastelli_type,
    avcanal_common_avv_regurgitation,
    avcanal_left_avv_regurgitation,
    avcanal_right_avv_regurgitation,
    avcanal_stenosis,
    avcanal_unbalanced,
    common_single_ventricle_av_valve_regurgitation
from sq_echo_study_inlets
