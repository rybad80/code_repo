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
        cast(avg(case when lower(name) = 'rpa_diam_s_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_rpa_diam_avg,
        cast(avg(case when lower(name) = 'rpa_diam_vs_bsa_boston_11_9_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_rpa_diam_zscore,
        cast(avg(case when lower(name) = 'rpa_vmax_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_rpa_vmax_avg,
        cast(avg(case when lower(name) = 'rpa_peak_grad_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_rpa_peak_gradient_avg,
        cast(avg(case when lower(name) = 'lpa_diam_s_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_lpa_diam_avg,
        cast(avg(case when lower(name) = 'lpa_vs_bsa_boston_11_9_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_lpa_diam_zscore,
        cast(avg(case when lower(name) = 'lpa_vmax_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_lpa_vmax_avg,
        cast(avg(case when lower(name) = 'lpa_peak_grad_calc' then floatvalue end) as decimal (28, 15)) as pulmonary_arteries_lpa_peak_gradient_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('rpa_diam_s_calc', 'rpa_diam_vs_bsa_boston_11_9_calc', 'rpa_vmax_calc', 'rpa_peak_grad_calc',
            'lpa_diam_s_calc', 'lpa_vs_bsa_boston_11_9_calc', 'lpa_vmax_calc', 'lpa_peak_grad_calc')
    group by
        studyid
),



sq_echo_study_pulmonary_arteries_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(pulmonary_arteries_rpa_diam_avg as numeric(28, 15)) as pulmonary_arteries_rpa_diam_avg,
        cast(pulmonary_arteries_rpa_diam_zscore as numeric(28, 15)) as pulmonary_arteries_rpa_diam_zscore,
        cast(pulmonary_arteries_rpa_vmax_avg as numeric(28, 15)) as pulmonary_arteries_rpa_vmax_avg,
        cast(pulmonary_arteries_rpa_peak_gradient_avg as numeric(28, 15)) as pulmonary_arteries_rpa_peak_gradient_avg,
        cast(pulmonary_arteries_lpa_diam_avg as numeric(28, 15)) as pulmonary_arteries_lpa_diam_avg,
        cast(pulmonary_arteries_lpa_diam_zscore as numeric(28, 15)) as pulmonary_arteries_lpa_diam_zscore,
        cast(pulmonary_arteries_lpa_vmax_avg as numeric(28, 15)) as pulmonary_arteries_lpa_vmax_avg,
        cast(pulmonary_arteries_lpa_peak_gradient_avg as numeric(28, 15)) as pulmonary_arteries_lpa_peak_gradient_avg
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    where (pulmonary_arteries_rpa_diam_avg is not null or pulmonary_arteries_rpa_diam_zscore is not null
        or pulmonary_arteries_rpa_vmax_avg is not null or pulmonary_arteries_rpa_peak_gradient_avg is not null
        or pulmonary_arteries_lpa_diam_avg is not null or pulmonary_arteries_lpa_diam_zscore is not null
        or pulmonary_arteries_lpa_vmax_avg is not null or pulmonary_arteries_lpa_peak_gradient_avg is not null)
)

select
    echo_study_id,
    pulmonary_arteries_rpa_diam_avg,
    pulmonary_arteries_rpa_diam_zscore,
    pulmonary_arteries_rpa_vmax_avg,
    pulmonary_arteries_rpa_peak_gradient_avg,
    pulmonary_arteries_lpa_diam_avg,
    pulmonary_arteries_lpa_diam_zscore,
    pulmonary_arteries_lpa_vmax_avg,
    pulmonary_arteries_lpa_peak_gradient_avg
from sq_echo_study_pulmonary_arteries_calcs
