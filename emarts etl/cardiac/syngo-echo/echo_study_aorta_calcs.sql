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
        cast(avg(case when lower(cdwfield) = 'aorta_pre_coarct_vmax_avg' then floatvalue end) as decimal (28, 15)) as aorta_pre_coarct_vmax_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_pre_coarct_peak_gradient_avg' then floatvalue end) as decimal (28, 15)) as aorta_pre_coarct_peak_gradient_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_desc_vmax_avg' then floatvalue end) as decimal (28, 15)) as aorta_desc_vmax_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_desc_peak_gradient_avg' then floatvalue end) as decimal (28, 15)) as aorta_desc_peak_gradient_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_coarct_corr_gradient_avg' then floatvalue end) as decimal (28, 15)) as aorta_coarct_corr_gradient_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_coarctation_mean_gradient_avg' then floatvalue end) as decimal (28, 15)) as aorta_coarctation_mean_gradient_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_ao_asc_avg' then floatvalue end) as decimal (28, 15)) as aorta_ao_asc_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_ao_asc_zscore' then floatvalue end) as decimal (28, 15)) as aorta_ao_asc_zscore,
        cast(avg(case when lower(cdwfield) = 'aorta_transverse_diam_avg' then floatvalue end) as decimal (28, 15)) as aorta_transverse_diam_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_transverse_diam_zscore' then floatvalue end) as decimal (28, 15)) as aorta_transverse_diam_zscore,
        cast(avg(case when lower(cdwfield) = 'aorta_isthmus_diam_avg' then floatvalue end) as decimal (28, 15)) as aorta_isthmus_diam_avg,
        cast(avg(case when lower(cdwfield) = 'aorta_isthmus_diam_zscore' then floatvalue end) as decimal (28, 15)) as aorta_isthmus_diam_zscore
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
        and echos.study_date between activedate and inactivedate
    where
        lower(cdwfield) in ('aorta_pre_coarct_vmax_avg', 'aorta_pre_coarct_peak_gradient_avg', 'aorta_desc_vmax_avg', 'aorta_desc_peak_gradient_avg', 'aorta_coarct_corr_gradient_avg', 'aorta_coarctation_mean_gradient_avg', 'aorta_ao_asc_avg', 'aorta_ao_asc_zscore', 'aorta_transverse_diam_avg', 'aorta_transverse_diam_zscore', 'aorta_isthmus_diam_avg', 'aorta_isthmus_diam_zscore')
    group by
        studyid
),

sq_echo_study_aorta_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(aorta_pre_coarct_vmax_avg as numeric(28, 15)) as aorta_pre_coarct_vmax_avg,
        cast(aorta_pre_coarct_peak_gradient_avg as numeric(28, 15)) as aorta_pre_coarct_peak_gradient_avg,
        cast(aorta_desc_vmax_avg as numeric(28, 15)) as aorta_desc_vmax_avg,
        cast(aorta_desc_peak_gradient_avg as numeric(28, 15)) as aorta_desc_peak_gradient_avg,
        cast(aorta_coarct_corr_gradient_avg as numeric(28, 15)) as aorta_coarct_corr_gradient_avg,
        cast(aorta_coarctation_mean_gradient_avg as numeric(28, 15)) as aorta_coarctation_mean_gradient_avg,
        cast(aorta_ao_asc_avg as numeric(28, 15)) as aorta_ao_asc_avg,
        cast(aorta_ao_asc_zscore as numeric(28, 15)) as aorta_ao_asc_zscore,
        cast(aorta_transverse_diam_avg as numeric(28, 15)) as aorta_transverse_diam_avg,
        cast(aorta_transverse_diam_zscore as numeric(28, 15)) as aorta_transverse_diam_zscore,
        cast(aorta_isthmus_diam_avg as numeric(28, 15)) as aorta_isthmus_diam_avg,
        cast(aorta_isthmus_diam_zscore as numeric(28, 15)) as aorta_isthmus_diam_zscore
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    where (aorta_pre_coarct_vmax_avg is not null or aorta_pre_coarct_peak_gradient_avg is not null
        or aorta_desc_vmax_avg is not null or aorta_desc_peak_gradient_avg is not null
        or aorta_coarct_corr_gradient_avg is not null or aorta_coarctation_mean_gradient_avg is not null
        or aorta_ao_asc_avg is not null or aorta_ao_asc_zscore is not null or aorta_transverse_diam_avg is not null
        or aorta_transverse_diam_zscore is not null or aorta_isthmus_diam_avg is not null
        or aorta_isthmus_diam_zscore is not null)
)
select
    echo_study_id,
    aorta_pre_coarct_vmax_avg,
    aorta_pre_coarct_peak_gradient_avg,
    aorta_desc_vmax_avg,
    aorta_desc_peak_gradient_avg,
    aorta_coarct_corr_gradient_avg,
    aorta_coarctation_mean_gradient_avg,
    aorta_ao_asc_avg,
    aorta_ao_asc_zscore,
    aorta_transverse_diam_avg,
    aorta_transverse_diam_zscore,
    aorta_isthmus_diam_avg,
    aorta_isthmus_diam_zscore
from sq_echo_study_aorta_calcs
