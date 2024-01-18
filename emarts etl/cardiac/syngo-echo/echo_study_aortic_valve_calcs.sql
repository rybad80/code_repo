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
        cast(avg(case when lower(cdwfield) = 'aortic_valve_ao_ann_diam_avg' then floatvalue end) as decimal (28, 15)) as aortic_valve_ao_ann_diam_avg,
        cast(avg(case when lower(cdwfield) = 'aortic_valve_ao_ann_diam_zscore' then floatvalue end) as decimal (28, 15)) as aortic_valve_ao_ann_diam_zscore,
        cast(avg(case when lower(cdwfield) = 'aortic_valve_ao_root_calc_avg' then floatvalue end) as decimal (28, 15)) as aortic_valve_ao_root_calc_avg,
        cast(avg(case when lower(cdwfield) = 'aortic_valve_ao_root_calc_zscore' then floatvalue end) as decimal (28, 15)) as aortic_valve_ao_root_calc_zscore,
        cast(avg(case when lower(cdwfield) = 'aortic_valve_ao_st_jnct_avg' then floatvalue end) as decimal (28, 15)) as aortic_valve_ao_st_jnct_avg,
        cast(avg(case when lower(cdwfield) = 'aortic_valve_ao_st_jnct_zscore' then floatvalue end) as decimal (28, 15)) as aortic_valve_ao_st_jnct_zscore,
        cast(avg(case when lower(cdwfield) = 'aortic_valve_peak_gradient_avg' then floatvalue end) as decimal (28, 15)) as aortic_valve_peak_gradient_avg,
        cast(avg(case when lower(cdwfield) = 'aortic_valve_mean_gradient_avg' then floatvalue end) as decimal (28, 15)) as aortic_valve_mean_gradient_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
        and echos.study_date between activedate and inactivedate
    where
        lower(cdwfield) in ('aortic_valve_ao_ann_diam_avg', 'aortic_valve_ao_ann_diam_zscore', 'aortic_valve_ao_root_calc_avg',
        'aortic_valve_ao_root_calc_zscore', 'aortic_valve_ao_st_jnct_avg', 'aortic_valve_ao_st_jnct_zscore',
        'aortic_valve_peak_gradient_avg', 'aortic_valve_mean_gradient_avg')
    group by
        studyid
),

sq_echo_aortic_valve_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(aortic_valve_ao_ann_diam_avg as numeric(28, 15)) as aortic_valve_ao_ann_diam_avg,
        cast(aortic_valve_ao_ann_diam_zscore as numeric(28, 15)) as aortic_valve_ao_ann_diam_zscore,
        cast(aortic_valve_ao_root_calc_avg as numeric(28, 15)) as aortic_valve_ao_root_calc_avg,
        cast(aortic_valve_ao_root_calc_zscore as numeric(28, 15)) as aortic_valve_ao_root_calc_zscore,
        cast(aortic_valve_ao_st_jnct_avg as numeric(28, 15)) as aortic_valve_ao_st_jnct_avg,
        cast(aortic_valve_ao_st_jnct_zscore as numeric(28, 15)) as aortic_valve_ao_st_jnct_zscore,
        cast(aortic_valve_peak_gradient_avg as numeric(28, 15)) as aortic_valve_peak_gradient_avg,
        cast(aortic_valve_mean_gradient_avg as numeric(28, 15)) as aortic_valve_mean_gradient_avg
    from echos
        left join measurements
            on echos.source_system_id = measurements.studyid
    where (aortic_valve_ao_ann_diam_avg is not null or aortic_valve_ao_ann_diam_zscore is not null
    or aortic_valve_ao_root_calc_avg is not null or aortic_valve_ao_root_calc_zscore is not null
    or aortic_valve_ao_st_jnct_avg is not null or aortic_valve_ao_st_jnct_zscore is not null
    or aortic_valve_peak_gradient_avg is not null or aortic_valve_mean_gradient_avg is not null)
)

select
    echo_study_id,
    aortic_valve_ao_ann_diam_avg,
    aortic_valve_ao_ann_diam_zscore,
    aortic_valve_ao_root_calc_avg,
    aortic_valve_ao_root_calc_zscore,
    aortic_valve_ao_st_jnct_avg,
    aortic_valve_ao_st_jnct_zscore,
    aortic_valve_peak_gradient_avg,
    aortic_valve_mean_gradient_avg
from sq_echo_aortic_valve_calcs
