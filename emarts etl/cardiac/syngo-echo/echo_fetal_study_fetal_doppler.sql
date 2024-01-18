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
    where lower(syngo_echo_observationname.name) in ('absent_ductus_venosus_obs', 'doppler_site_name_1_obs', 'doppler_site_name_3_obs',
        'doppler_site_name_2_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'absent_ductus_venosus_obs' then displayvalue end as absent_ductus_venosus_obs,
        case when observation_name = 'doppler_site_name_1_obs' then displayvalue end as doppler_site_name_1_obs,
        case when observation_name = 'doppler_site_name_3_obs' then displayvalue end as doppler_site_name_3_obs,
    case when observation_name = 'doppler_site_name_2_obs' then displayvalue end as doppler_site_name_2_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(absent_ductus_venosus_obs, ';') as absent_ductus_venosus_obs,
        group_concat(doppler_site_name_1_obs, ';') as doppler_site_name_1_obs,
        group_concat(doppler_site_name_2_obs, ';') as doppler_site_name_2_obs,
        group_concat(doppler_site_name_3_obs, ';') as doppler_site_name_3_obs
    from observations
    group by studyid, ownerid
),

measurements as (
    select
        studyid,
        ownerid,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_2_pi_calc' then floatvalue end) as decimal (27, 12)) as dop_site_2_pi_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_2_tav_calc' then floatvalue end) as decimal (27, 12)) as dop_site_2_tav_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_1_tav_calc' then floatvalue end) as decimal (27, 12)) as dop_site_1_tav_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_3_pi_calc' then floatvalue end) as decimal (27, 12)) as dop_site_3_pi_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_2_max_calc' then floatvalue end) as decimal (27, 12)) as dop_site_2_max_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_3_min_calc' then floatvalue end) as decimal (27, 12)) as dop_site_3_min_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_3_max_calc' then floatvalue end) as decimal (27, 12)) as dop_site_3_max_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_2_min_calc' then floatvalue end) as decimal (27, 12)) as dop_site_2_min_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_1_min_calc' then floatvalue end) as decimal (27, 12)) as dop_site_1_min_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_1_pi_calc' then floatvalue end) as decimal (27, 12)) as dop_site_1_pi_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_1_max_calc' then floatvalue end) as decimal (27, 12)) as dop_site_1_max_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'dop_site_3_tav_calc' then floatvalue end) as decimal (27, 12)) as dop_site_3_tav_calc
    from {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype
        on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    where
        lower(syngo_echo_measurementtype.name) in ('dop_site_1_max_calc', 'dop_site_1_min_calc', 'dop_site_1_pi_calc', 'dop_site_1_tav_calc',
        'dop_site_2_max_calc', 'dop_site_2_min_calc', 'dop_site_2_pi_calc', 'dop_site_2_tav_calc', 'dop_site_3_max_calc',
        'dop_site_3_min_calc', 'dop_site_3_pi_calc', 'dop_site_3_tav_calc')
    group by
        studyid,
        ownerid
),


sq_echo_fetal_study_fetal_doppler as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        absent_ductus_venosus_obs as fetal_doppler_absent_ductus_venosus,
        doppler_site_name_1_obs as fetal_doppler_site_1_name,
        dop_site_1_max_calc as fetal_doppler_site_1_peak_systolic_velocity_avg,
        dop_site_1_min_calc as fetal_doppler_site_1_end_diastolic_volume_avg,
        dop_site_1_tav_calc as fetal_doppler_site_1_time_average_mean_avg,
        dop_site_1_pi_calc as fetal_doppler_site_1_pulsatility_index_avg,
        doppler_site_name_2_obs as fetal_doppler_site_2_name,
        dop_site_2_max_calc as fetal_doppler_site_2_peak_systolic_velocity_avg,
        dop_site_2_min_calc as fetal_doppler_site_2_end_diastolic_volume_avg,
        dop_site_2_tav_calc as fetal_doppler_site_2_time_average_mean_avg,
        dop_site_2_pi_calc as fetal_doppler_site_2_pulsatility_index_avg,
        doppler_site_name_3_obs as fetal_doppler_site_3_name,
        dop_site_3_max_calc as fetal_doppler_site_3_peak_systolic_velocity_avg,
        dop_site_3_min_calc as fetal_doppler_site_3_end_diastolic_volume_avg,
        dop_site_3_tav_calc as fetal_doppler_site_3_time_average_mean_avg,
        dop_site_3_pi_calc as fetal_doppler_site_3_pulsatility_index_avg
    from fetal_echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study_owner') }} as syngo_echo_study_owner
        on fetal_echos.source_system_id = syngo_echo_study_owner.study_ref
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
            and syngo_echo_study_owner.ownerid = observation_group_1.ownerid
    left join measurements
        on fetal_echos.source_system_id = measurements.studyid
            and syngo_echo_study_owner.ownerid = measurements.ownerid
    where syngo_echo_study_owner.ownertype in (1, 2)
        and (absent_ductus_venosus_obs is not null or dop_site_1_max_calc is not null or dop_site_1_min_calc is not null or dop_site_1_pi_calc is not null or dop_site_1_tav_calc is not null or dop_site_2_max_calc is not null or dop_site_2_min_calc is not null or dop_site_2_pi_calc is not null or dop_site_2_tav_calc is not null or dop_site_3_max_calc is not null or dop_site_3_min_calc is not null or dop_site_3_pi_calc is not null or dop_site_3_tav_calc is not null or doppler_site_name_1_obs is not null or doppler_site_name_2_obs is not null or doppler_site_name_3_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(fetal_doppler_absent_ductus_venosus as varchar(255)) as fetal_doppler_absent_ductus_venosus,
    cast(fetal_doppler_site_1_name as varchar(255)) as fetal_doppler_site_1_name,
    cast(fetal_doppler_site_1_peak_systolic_velocity_avg as numeric(28, 15)) as fetal_doppler_site_1_peak_systolic_velocity_avg,
    cast(fetal_doppler_site_1_end_diastolic_volume_avg as numeric(28, 15)) as fetal_doppler_site_1_end_diastolic_volume_avg,
    cast(fetal_doppler_site_1_time_average_mean_avg as numeric(28, 15)) as fetal_doppler_site_1_time_average_mean_avg,
    cast(fetal_doppler_site_1_pulsatility_index_avg as numeric(28, 15)) as fetal_doppler_site_1_pulsatility_index_avg,
    cast(fetal_doppler_site_2_name as varchar(255)) as fetal_doppler_site_2_name,
    cast(fetal_doppler_site_2_peak_systolic_velocity_avg as numeric(28, 15)) as fetal_doppler_site_2_peak_systolic_velocity_avg,
    cast(fetal_doppler_site_2_end_diastolic_volume_avg as numeric(28, 15)) as fetal_doppler_site_2_end_diastolic_volume_avg,
    cast(fetal_doppler_site_2_time_average_mean_avg as numeric(28, 15)) as fetal_doppler_site_2_time_average_mean_avg,
    cast(fetal_doppler_site_2_pulsatility_index_avg as numeric(28, 15)) as fetal_doppler_site_2_pulsatility_index_avg,
    cast(fetal_doppler_site_3_name as varchar(255)) as fetal_doppler_site_3_name,
    cast(fetal_doppler_site_3_peak_systolic_velocity_avg as numeric(28, 15)) as fetal_doppler_site_3_peak_systolic_velocity_avg,
    cast(fetal_doppler_site_3_end_diastolic_volume_avg as numeric(28, 15)) as fetal_doppler_site_3_end_diastolic_volume_avg,
    cast(fetal_doppler_site_3_time_average_mean_avg as numeric(28, 15)) as fetal_doppler_site_3_time_average_mean_avg,
    cast(fetal_doppler_site_3_pulsatility_index_avg as numeric(28, 15)) as fetal_doppler_site_3_pulsatility_index_avg
from sq_echo_fetal_study_fetal_doppler
