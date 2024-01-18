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
        cast(avg(case when lower(name) = 'chop_residual_arch_obstruction_vmax_calc' then floatvalue end) as decimal (28, 15)) as aorta_surgery_vmax_avg,
        cast(avg(case when lower(name) = 'chop_residual_arch_obstruction_pk_grad_calc' then floatvalue end) as decimal (28, 15)) as aorta_surgery_peak_gradient_avg,
        cast(avg(case when lower(name) = 'chop_residual_arch_obstruction_mn_grad_calc' then floatvalue end) as decimal (28, 15)) as aorta_surgery_mean_gradient_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('chop_residual_arch_obstruction_vmax_calc', 'chop_residual_arch_obstruction_pk_grad_calc', 'chop_residual_arch_obstruction_mn_grad_calc')
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
    where lower(syngo_echo_observationname.name) in ('residual_lt_avvr_obs', 'residual_rt_avvr_obs',
        'residual_lt_avvs_obs', 'residual_rt_avvs_obs')
),
observations as (
    select
        studyid,
        case when observation_name = 'residual_lt_avvr_obs' then displayvalue end as av_canal_surgery_residual_left_avvr,
        case when observation_name = 'residual_rt_avvr_obs' then displayvalue end as av_canal_surgery_residual_right_avvr,
        case when observation_name = 'residual_lt_avvs_obs' then displayvalue end as av_canal_surgery_residual_left_avvs,
        case when observation_name = 'residual_rt_avvs_obs' then displayvalue end as av_canal_surgery_residual_right_avvs
    from observation_display_values
),
observation_group as (
    select
        studyid,
        group_concat(av_canal_surgery_residual_left_avvr, ';') as av_canal_surgery_residual_left_avvr,
        group_concat(av_canal_surgery_residual_right_avvr, ';') as av_canal_surgery_residual_right_avvr,
        group_concat(av_canal_surgery_residual_left_avvs, ';') as av_canal_surgery_residual_left_avvs,
        group_concat(av_canal_surgery_residual_right_avvs, ';') as av_canal_surgery_residual_right_avvs
    from observations
    group by studyid
),
sq_echo_study_surgeries as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(av_canal_surgery_residual_left_avvr as varchar(255)) as av_canal_surgery_residual_left_avvr,
        cast(av_canal_surgery_residual_right_avvr as varchar(255)) as av_canal_surgery_residual_right_avvr,
        cast(av_canal_surgery_residual_left_avvs as varchar(255)) as av_canal_surgery_residual_left_avvs,
        cast(av_canal_surgery_residual_right_avvs as varchar(255)) as av_canal_surgery_residual_right_avvs,
        cast(aorta_surgery_vmax_avg as numeric(28, 15)) as aorta_surgery_vmax_avg,
        cast(aorta_surgery_peak_gradient_avg as numeric(28, 15)) as aorta_surgery_peak_gradient_avg,
        cast(aorta_surgery_mean_gradient_avg as numeric(28, 15)) as aorta_surgery_mean_gradient_avg
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    left join observation_group
        on echos.source_system_id = observation_group.studyid
where
    (av_canal_surgery_residual_left_avvr is not null or av_canal_surgery_residual_right_avvr is not null
    or av_canal_surgery_residual_left_avvs is not null or av_canal_surgery_residual_right_avvs is not null
    or aorta_surgery_vmax_avg is not null or aorta_surgery_peak_gradient_avg is not null
    or aorta_surgery_mean_gradient_avg is not null)
)
select
    echo_study_id,
    av_canal_surgery_residual_left_avvr,
    av_canal_surgery_residual_right_avvr,
    av_canal_surgery_residual_left_avvs,
    av_canal_surgery_residual_right_avvs,
    aorta_surgery_vmax_avg,
    aorta_surgery_peak_gradient_avg,
    aorta_surgery_mean_gradient_avg
from sq_echo_study_surgeries
