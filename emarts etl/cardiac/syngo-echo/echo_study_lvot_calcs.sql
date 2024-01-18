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
        cast(avg(case when lower(name) = 'lvot_peak_grad_calc' then floatvalue end) as decimal (28, 15)) as lvot_peak_gradient_avg,
        cast(avg(case when lower(name) = 'lvot_mean_grad_calc' then floatvalue end) as decimal (28, 15)) as lvot_mean_gradient_avg
from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('lvot_peak_grad_calc', 'lvot_mean_grad_calc')
    group by
        studyid
),

sq_echo_study_lvot_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(lvot_peak_gradient_avg as numeric(28, 15)) as lvot_peak_gradient_avg,
        cast(lvot_mean_gradient_avg as numeric(28, 15)) as lvot_mean_gradient_avg
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    where (lvot_peak_gradient_avg is not null or lvot_mean_gradient_avg is not null)
)

select
    echo_study_id,
    lvot_peak_gradient_avg,
    lvot_mean_gradient_avg
from sq_echo_study_lvot_calcs
