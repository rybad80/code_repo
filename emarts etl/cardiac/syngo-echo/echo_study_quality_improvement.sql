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
        cast(min(case when lower(name) = 'time_on_last_image_dt' then datetimevalue end) as timestamp) as time_on_last_image_dt
    from echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue on echos.source_system_id = syngo_echo_measurementvalue.studyid
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_measurementtype.name
    where
        lower(name) in ('time_on_last_image_dt')
    group by
        studyid
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
    where lower(syngo_echo_observationname.name) in ('qi_project_name_obs', 'qi_order_obs', 'qi_patient_factors_obs',
        'qi_communication_obs', 'qi_additional_images_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'qi_project_name_obs' then displayvalue end as qi_project_name,
        case when observation_name = 'qi_order_obs' then displayvalue end as qi_order,
        case when observation_name = 'qi_patient_factors_obs' then displayvalue end as qi_patient_factors,
        case when observation_name = 'qi_communication_obs' then displayvalue end as qi_communication,
        case when observation_name = 'qi_additional_images_obs' then displayvalue end as qi_additional_images
    from observation_display_values
),

observation_group as (
    select
        studyid,
        group_concat(qi_project_name, ';') as qi_project_name,
        group_concat(qi_order, ';') as qi_order,
        group_concat(qi_patient_factors, ';') as qi_patient_factors,
        group_concat(qi_communication, ';') as qi_communication,
        group_concat(qi_additional_images, ';') as qi_additional_images
    from observations
    group by studyid
),

sq_echo_study_quality_improvement as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(qi_project_name as varchar(255)) as qi_project_name,
        cast(study_date as date) + cast(time_on_last_image_dt as time) as time_on_last_image,
        cast(qi_order as varchar(255)) as qi_order,
        cast(qi_patient_factors as varchar(255)) as qi_patient_factors,
        cast(qi_communication as varchar(255)) as qi_communication,
        cast(qi_additional_images as varchar(255)) as qi_additional_images
    from echos
    left join measurements
        on echos.source_system_id = measurements.studyid
    left join observation_group
        on echos.source_system_id = observation_group.studyid
    where (qi_project_name is not null or time_on_last_image is not null or qi_order is not null
        or qi_patient_factors is not null or qi_communication is not null or qi_additional_images is not null
    )
)

select
    echo_study_id,
    qi_project_name,
    time_on_last_image,
    qi_order,
    qi_patient_factors,
    qi_communication,
    qi_additional_images
from sq_echo_study_quality_improvement
