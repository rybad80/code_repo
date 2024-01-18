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
        cast(avg(case when lower(name) = 'tapse_calc' then floatvalue end) as decimal (28, 15)) as right_ventricle_tapse_mmode_avg,
        cast(avg(case when lower(name) = 'tv_ann_plane_syst_exc_calc' then floatvalue end) as decimal (28, 15)) as right_ventricle_tapse_2d_avg,
        cast(avg(case when lower(name) = 'rv_e_calc' then floatvalue end) as decimal (28, 15)) as right_ventricle_e_free_wall_avg,
        cast(avg(case when lower(name) = 'rv_e_e_calc' then floatvalue end) as decimal (28, 15)) as right_ventricle_e_e_prime_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('tapse_calc', 'tv_ann_plane_syst_exc_calc', 'rv_e_calc', 'rv_e_e_calc')
    group by
        studyid
),
sq_echo_study_right_ventricle_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(right_ventricle_tapse_mmode_avg as numeric(28, 15)) as right_ventricle_tapse_mmode_avg,
        cast(right_ventricle_tapse_2d_avg as numeric(28, 15)) as right_ventricle_tapse_2d_avg,
        cast(right_ventricle_e_free_wall_avg as numeric(28, 15)) as right_ventricle_e_free_wall_avg,
        cast(right_ventricle_e_e_prime_avg as numeric(28, 15)) as right_ventricle_e_e_prime_avg
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    where (right_ventricle_tapse_mmode_avg is not null or right_ventricle_tapse_2d_avg is not null
    or right_ventricle_e_free_wall_avg is not null or right_ventricle_e_e_prime_avg is not null)
)


select
    echo_study_id,
    right_ventricle_tapse_mmode_avg,
    right_ventricle_tapse_2d_avg,
    right_ventricle_e_free_wall_avg,
    right_ventricle_e_e_prime_avg
from sq_echo_study_right_ventricle_calcs
