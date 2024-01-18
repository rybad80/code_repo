with echos as (
    select
        source_system_id,
        echo_study_id,
        to_date(study_date_key, 'yyyymmdd') as study_date
    from {{ ref('echo_study') }}
    where lower(source_system) = 'syngo'
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
    where lower(syngo_echo_observationname.name) in ('chw_ra_size_obs', 'chw_la_size_obs', 'pab_asu_residual_asd_size_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'chw_ra_size_obs' then displayvalue end as chw_ra_size_obs,
        case when observation_name = 'chw_la_size_obs' then displayvalue end as chw_la_size_obs,
        case when observation_name = 'pab_asu_residual_asd_size_obs' then displayvalue end as pab_asu_residual_asd_size_obs
    from observation_display_values
),

observation_group as (
    select
        studyid,
        group_concat(chw_ra_size_obs, ';') as right_atrium_size,
        group_concat(chw_la_size_obs, ';') as left_atrium_size,
        group_concat(pab_asu_residual_asd_size_obs, ';') as residual_asd
    from observations
    group by studyid
),

sq_echo_study_atria as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(right_atrium_size as varchar(255)) as right_atrium_size,
        cast(left_atrium_size as varchar(255)) as left_atrium_size,
        cast(residual_asd as varchar(255)) as residual_asd
    from echos
    left join observation_group
        on echos.source_system_id = observation_group.studyid
where
    (right_atrium_size is not null or left_atrium_size is not null or residual_asd is not null)
)

select
    echo_study_id,
    right_atrium_size,
    left_atrium_size,
    residual_asd
from sq_echo_study_atria
