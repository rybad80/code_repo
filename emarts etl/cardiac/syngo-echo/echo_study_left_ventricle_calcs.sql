with echos as (
    select
        source_system_id,
        echo_study_id,
        to_date(study_date_key, 'yyyymmdd') as study_date
    from {{ ref('echo_study') }}
    where lower(source_system) = 'syngo'
),

 measurements as (
    select
        studyid,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_ef_mmode_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_ef_mmode_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_ef_3d_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_ef_3d_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvid_d_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvid_d_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvid_d_zscore' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvid_d_zscore,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvid_s_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvid_s_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvid_s_zscore' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvid_s_zscore,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvsf' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvsf,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvpwd_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvpwd_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvpwd_zscore' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvpwd_zscore,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvpws_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvpws_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_lvpws_zscore' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_lvpws_zscore,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_ivs_d_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_ivs_d_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_ivs_d_zscore' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_ivs_d_zscore,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_ivs_s_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_ivs_s_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mmode_ivs_s_zscore' then floatvalue end) as decimal (28, 15)) as left_ventricle_mmode_ivs_s_zscore,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_volume_d_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_volume_d_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_volume_s_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_volume_s_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mass_mmode_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_mass_mmode_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_mass_zscore' then floatvalue end) as decimal (28, 15)) as left_ventricle_mass_zscore,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_ef_a4c_avg' then floatvalue end) as decimal (28, 15)) as left_ventricle_ef_a4c_avg,
        cast(avg(case when lower(cdwfield) = 'left_ventricle_ef_bip' then floatvalue end) as decimal (28, 15)) as left_ventricle_ef_bip,
        cast(avg(case when lower(cdwfield) = 'diastolic_function_medial_e_prime_avg' then floatvalue end) as decimal (28, 15)) as diastolic_function_medial_e_prime_avg,
        cast(avg(case when lower(cdwfield) = 'diastolic_function_lateral_e_prime_avg' then floatvalue end) as decimal (28, 15)) as diastolic_function_lateral_e_prime_avg,
        cast(avg(case when lower(cdwfield) = 'diastolic_function_e_e_prime_medial_avg' then floatvalue end) as decimal (28, 15)) as diastolic_function_e_e_prime_medial_avg,
        cast(avg(case when lower(cdwfield) = 'diastolic_function_e_e_prime_lateral_avg' then floatvalue end) as decimal (28, 15)) as diastolic_function_e_e_prime_lateral_avg,
        cast(avg(case when lower(cdwfield) = 'volume_d_4chamber_avg' then floatvalue end) as decimal (28, 15)) as volume_d_4chamber_avg,
        cast(avg(case when lower(cdwfield) = 'volume_s_4chamber_avg' then floatvalue end) as decimal (28, 15)) as volume_s_4chamber_avg,
        cast(avg(case when lower(cdwfield) = 'volume_index_d_4chamber_avg' then floatvalue end) as decimal (28, 15)) as volume_index_d_4chamber_avg,
        cast(avg(case when lower(cdwfield) = 'volume_index_s_4chamber_avg' then floatvalue end) as decimal (28, 15)) as volume_index_s_4chamber_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
        and echos.study_date between activedate and inactivedate
    where
        lower(cdwfield) in ('left_ventricle_ef_mmode_avg', 'left_ventricle_ef_3d_avg', 'left_ventricle_mmode_lvid_d_avg',
            'left_ventricle_mmode_lvid_d_zscore', 'left_ventricle_mmode_lvid_s_avg', 'left_ventricle_mmode_lvid_s_zscore',
            'left_ventricle_mmode_lvsf', 'left_ventricle_mmode_lvpwd_avg', 'left_ventricle_mmode_lvpwd_zscore',
            'left_ventricle_mmode_lvpws_avg', 'left_ventricle_mmode_lvpws_zscore', 'left_ventricle_mmode_ivs_d_avg',
            'left_ventricle_mmode_ivs_d_zscore', 'left_ventricle_mmode_ivs_s_avg', 'left_ventricle_mmode_ivs_s_zscore',
            'left_ventricle_volume_d_avg', 'left_ventricle_volume_s_avg', 'left_ventricle_mass_mmode_avg', 'left_ventricle_mass_zscore',
            'left_ventricle_ef_a4c_avg', 'left_ventricle_ef_bip', 'diastolic_function_medial_e_prime_avg',
            'diastolic_function_lateral_e_prime_avg', 'diastolic_function_e_e_prime_medial_avg',
            'diastolic_function_e_e_prime_lateral_avg', 'volume_d_4chamber_avg', 'volume_s_4chamber_avg',
            'volume_index_d_4chamber_avg', 'volume_index_s_4chamber_avg')
    group by
        studyid
),

sq_echo_study_left_ventricle_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(left_ventricle_ef_mmode_avg as numeric(28, 15)) as left_ventricle_ef_mmode_avg,
        cast(left_ventricle_ef_3d_avg as numeric(28, 15)) as left_ventricle_ef_3d_avg,
        cast(left_ventricle_mmode_lvid_d_avg as numeric(28, 15)) as left_ventricle_mmode_lvid_d_avg,
        cast(left_ventricle_mmode_lvid_d_zscore as numeric(28, 15)) as left_ventricle_mmode_lvid_d_zscore,
        cast(left_ventricle_mmode_lvid_s_avg as numeric(28, 15)) as left_ventricle_mmode_lvid_s_avg,
        cast(left_ventricle_mmode_lvid_s_zscore as numeric(28, 15)) as left_ventricle_mmode_lvid_s_zscore,
        cast(left_ventricle_mmode_lvsf as numeric(28, 15)) as left_ventricle_mmode_lvsf,
        cast(left_ventricle_mmode_lvpwd_avg as numeric(28, 15)) as left_ventricle_mmode_lvpwd_avg,
        cast(left_ventricle_mmode_lvpwd_zscore as numeric(28, 15)) as left_ventricle_mmode_lvpwd_zscore,
        cast(left_ventricle_mmode_lvpws_avg as numeric(28, 15)) as left_ventricle_mmode_lvpws_avg,
        cast(left_ventricle_mmode_lvpws_zscore as numeric(28, 15)) as left_ventricle_mmode_lvpws_zscore,
        cast(left_ventricle_mmode_ivs_d_avg as numeric(28, 15)) as left_ventricle_mmode_ivs_d_avg,
        cast(left_ventricle_mmode_ivs_d_zscore as numeric(28, 15)) as left_ventricle_mmode_ivs_d_zscore,
        cast(left_ventricle_mmode_ivs_s_avg as numeric(28, 15)) as left_ventricle_mmode_ivs_s_avg,
        cast(left_ventricle_mmode_ivs_s_zscore as numeric(28, 15)) as left_ventricle_mmode_ivs_s_zscore,
        cast(left_ventricle_volume_d_avg as numeric(28, 15)) as left_ventricle_volume_d_avg,
        cast(left_ventricle_volume_s_avg as numeric(28, 15)) as left_ventricle_volume_s_avg,
        cast(left_ventricle_mass_mmode_avg as numeric(28, 15)) as left_ventricle_mass_mmode_avg,
        cast(left_ventricle_mass_zscore as numeric(28, 15)) as left_ventricle_mass_zscore,
        cast(left_ventricle_ef_a4c_avg as numeric(28, 15)) as left_ventricle_ef_a4c_avg,
        cast(left_ventricle_ef_bip as numeric(28, 15)) as left_ventricle_ef_bip,
        cast(diastolic_function_medial_e_prime_avg as numeric(28, 15)) as diastolic_function_medial_e_prime_avg,
        cast(diastolic_function_lateral_e_prime_avg as numeric(28, 15)) as diastolic_function_lateral_e_prime_avg,
        cast(diastolic_function_e_e_prime_medial_avg as numeric(28, 15)) as diastolic_function_e_e_prime_medial_avg,
        cast(diastolic_function_e_e_prime_lateral_avg as numeric(28, 15)) as diastolic_function_e_e_prime_lateral_avg,
        cast(volume_d_4chamber_avg as numeric(28, 15)) as volume_d_4chamber_avg,
        cast(volume_s_4chamber_avg as numeric(28, 15)) as volume_s_4chamber_avg,
        cast(volume_index_d_4chamber_avg as numeric(28, 15)) as volume_index_d_4chamber_avg,
        cast(volume_index_s_4chamber_avg as numeric(28, 15)) as volume_index_s_4chamber_avg
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    where (left_ventricle_ef_mmode_avg is not null or left_ventricle_ef_3d_avg is not null or left_ventricle_mmode_lvid_d_avg is not null
        or left_ventricle_mmode_lvid_d_zscore is not null or left_ventricle_mmode_lvid_s_avg is not null
        or left_ventricle_mmode_lvid_s_zscore is not null or left_ventricle_mmode_lvsf is not null
        or left_ventricle_mmode_lvpwd_avg is not null or left_ventricle_mmode_lvpwd_zscore is not null
        or left_ventricle_mmode_lvpws_avg is not null or left_ventricle_mmode_lvpws_zscore is not null
        or left_ventricle_mmode_ivs_d_avg is not null or left_ventricle_mmode_ivs_d_zscore is not null
        or left_ventricle_mmode_ivs_s_avg is not null or left_ventricle_mmode_ivs_s_zscore is not null
        or left_ventricle_volume_d_avg is not null or left_ventricle_volume_s_avg is not null
        or left_ventricle_mass_mmode_avg is not null or left_ventricle_mass_zscore is not null
        or left_ventricle_ef_a4c_avg is not null or left_ventricle_ef_bip is not null
        or diastolic_function_medial_e_prime_avg is not null or diastolic_function_lateral_e_prime_avg is not null
        or diastolic_function_e_e_prime_medial_avg is not null or diastolic_function_e_e_prime_lateral_avg is not null
        or volume_d_4chamber_avg is not null or volume_s_4chamber_avg is not null or volume_index_d_4chamber_avg is not null
        or volume_index_s_4chamber_avg is not null)
)

select
    echo_study_id,
    left_ventricle_ef_mmode_avg,
    left_ventricle_ef_3d_avg,
    left_ventricle_mmode_lvid_d_avg,
    left_ventricle_mmode_lvid_d_zscore,
    left_ventricle_mmode_lvid_s_avg,
    left_ventricle_mmode_lvid_s_zscore,
    left_ventricle_mmode_lvsf,
    left_ventricle_mmode_lvpwd_avg,
    left_ventricle_mmode_lvpwd_zscore,
    left_ventricle_mmode_lvpws_avg,
    left_ventricle_mmode_lvpws_zscore,
    left_ventricle_mmode_ivs_d_avg,
    left_ventricle_mmode_ivs_d_zscore,
    left_ventricle_mmode_ivs_s_avg,
    left_ventricle_mmode_ivs_s_zscore,
    left_ventricle_volume_d_avg,
    left_ventricle_volume_s_avg,
    left_ventricle_mass_mmode_avg,
    left_ventricle_mass_zscore,
    left_ventricle_ef_a4c_avg,
    left_ventricle_ef_bip,
    diastolic_function_medial_e_prime_avg,
    diastolic_function_lateral_e_prime_avg,
    diastolic_function_e_e_prime_medial_avg,
    diastolic_function_e_e_prime_lateral_avg,
    volume_d_4chamber_avg,
    volume_s_4chamber_avg,
    volume_index_d_4chamber_avg,
    volume_index_s_4chamber_avg
from sq_echo_study_left_ventricle_calcs
