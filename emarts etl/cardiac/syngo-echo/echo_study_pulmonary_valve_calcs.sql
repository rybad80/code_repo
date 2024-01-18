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
        cast(avg(case when lower(name) = 'pv_pk_grad_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_valve_peak_gradient_avg,
        cast(avg(case when lower(name) = 'pv_mean_grad_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_valve_mean_gradient_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('pv_pk_grad_calc', 'pv_mean_grad_calc')
    group by
        studyid
),

sq_echo_study_pulmonary_valve_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(pulmonary_valve_peak_gradient_avg as numeric(28, 15)) as pulmonary_valve_peak_gradient_avg,
        cast(pulmonary_valve_mean_gradient_avg as numeric(28, 15)) as pulmonary_valve_mean_gradient_avg
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    where (pulmonary_valve_peak_gradient_avg is not null or pulmonary_valve_mean_gradient_avg is not null)
)

select
    echo_study_id,
    pulmonary_valve_peak_gradient_avg,
    pulmonary_valve_mean_gradient_avg
from sq_echo_study_pulmonary_valve_calcs
