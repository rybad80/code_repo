with echos as (
    select
        source_system_id,
        echo_study_id
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
    where lower(syngo_echo_observationname.name) in ('chw_pe_no_obs', 'chw_pe_size_obs', 'chw_pe_location_descr_80904_obs')
),
observations as (
    select
        studyid,
        case when observation_name = 'chw_pe_no_obs' then displayvalue end as pericardial_effusion,
        case when observation_name = 'chw_pe_size_obs' then displayvalue end as pericardial_size,
        case when observation_name = 'chw_pe_location_descr_80904_obs' then displayvalue end as pericardial_location
    from observation_display_values
),

observation_group as (
    select
        studyid,
        group_concat(pericardial_effusion, ';') as pericardial_effusion,
        group_concat(pericardial_size, ';') as pericardial_size,
        group_concat(pericardial_location, ';') as pericardial_location
    from observations
    group by studyid
),

sq_echo_study_other as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(pericardial_effusion as varchar(255)) as pericardial_effusion,
        cast(pericardial_size as varchar(255)) as pericardial_size,
        cast(pericardial_location as varchar(255)) as pericardial_location
    from echos
    left join observation_group
        on echos.source_system_id = observation_group.studyid
    where (pericardial_effusion is not null or pericardial_size is not null or pericardial_location is not null)
)

select
    echo_study_id,
    pericardial_effusion,
    pericardial_size,
    pericardial_location
from sq_echo_study_other
