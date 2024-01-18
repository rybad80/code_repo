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
        cast(avg(case when lower(cdwfield) = 'mitral_ann_diam_d_4ac_avg' then floatvalue end) as decimal (28, 15)) as mitral_ann_diam_d_4ac_avg,
        cast(avg(case when lower(cdwfield) = 'mitral_ann_diam_d_4ac_zscore' then floatvalue end) as decimal (28, 15)) as mitral_ann_diam_d_4ac_zscore,
        cast(avg(case when lower(cdwfield) = 'mitral_ann_diam_d_ap_avg' then floatvalue end) as decimal (28, 15)) as mitral_ann_diam_d_ap_avg,
        cast(avg(case when lower(cdwfield) = 'mitral_ann_diam_d_ap_zscore' then floatvalue end) as decimal (28, 15)) as mitral_ann_diam_d_ap_zscore,
        cast(avg(case when lower(cdwfield) = 'mitral_area_avg' then floatvalue end) as decimal (28, 15)) as mitral_area_avg,
        cast(avg(case when lower(cdwfield) = 'mitral_e_v_max_avg' then floatvalue end) as decimal (28, 15)) as mitral_e_v_max_avg,
        cast(avg(case when lower(cdwfield) = 'mitral_a_v_max_avg' then floatvalue end) as decimal (28, 15)) as mitral_a_v_max_avg,
        cast(avg(case when lower(cdwfield) = 'mitral_e_a_inflow_avg' then floatvalue end) as decimal (28, 15)) as mitral_e_a_inflow_avg
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
        and echos.study_date between activedate and inactivedate
    where
        lower(cdwfield) in ('mitral_ann_diam_d_4ac_avg', 'mitral_ann_diam_d_4ac_zscore', 'mitral_ann_diam_d_ap_avg', 'mitral_ann_diam_d_ap_zscore', 'mitral_area_avg', 'mitral_e_v_max_avg', 'mitral_a_v_max_avg', 'mitral_e_a_inflow_avg')
    group by
        studyid
),

sq_echo_study_mitral_calcs as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(mitral_ann_diam_d_4ac_avg as numeric(28, 15)) as mitral_ann_diam_d_4ac_avg,
        cast(mitral_ann_diam_d_4ac_zscore as numeric(28, 15)) as mitral_ann_diam_d_4ac_zscore,
        cast(mitral_ann_diam_d_ap_avg as numeric(28, 15)) as mitral_ann_diam_d_ap_avg,
        cast(mitral_ann_diam_d_ap_zscore as numeric(28, 15)) as mitral_ann_diam_d_ap_zscore,
        cast(mitral_area_avg as numeric(28, 15)) as mitral_area_avg,
        cast(mitral_e_v_max_avg as numeric(28, 15)) as mitral_e_v_max_avg,
        cast(mitral_a_v_max_avg as numeric(28, 15)) as mitral_a_v_max_avg,
        cast(mitral_e_a_inflow_avg as numeric(28, 15)) as mitral_e_a_inflow_avg
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    where (mitral_ann_diam_d_4ac_avg is not null or mitral_ann_diam_d_4ac_zscore is not null
        or mitral_ann_diam_d_ap_avg is not null or mitral_ann_diam_d_ap_zscore is not null
        or mitral_area_avg is not null or mitral_e_v_max_avg is not null or mitral_a_v_max_avg is not null
        or mitral_e_a_inflow_avg is not null)
)

select
    echo_study_id,
    mitral_ann_diam_d_4ac_avg,
    mitral_ann_diam_d_4ac_zscore,
    mitral_ann_diam_d_ap_avg,
    mitral_ann_diam_d_ap_zscore,
    mitral_area_avg,
    mitral_e_v_max_avg,
    mitral_a_v_max_avg,
    mitral_e_a_inflow_avg
from sq_echo_study_mitral_calcs
