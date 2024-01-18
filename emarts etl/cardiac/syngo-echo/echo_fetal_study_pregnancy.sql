with fetal_echos as (
    select
        source_system_id,
        echo_fetal_study_id
    from {{ ref('echo_fetal_study') }}
    where lower(source_system) = 'syngo'
),

observation_display_values as (
    select
        syngo_echo_observationvalue.studyid,
        lower(syngo_echo_observationname.name) as observation_name,
        isnull(cast(syngo_echo_fetalobservationfieldmap.worksheetvalue as varchar(400)),
            cast(syngo_echo_observationvalue.val as varchar(400))) as displayvalue
    from {{ source('syngo_echo_ods', 'syngo_echo_observationvalue') }} as syngo_echo_observationvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_observationname') }} as syngo_echo_observationname
        on syngo_echo_observationvalue.observationid = syngo_echo_observationname.id
    left join {{ source('syngo_echo_ods', 'syngo_echo_fetalobservationfieldmap') }} as syngo_echo_fetalobservationfieldmap
        on syngo_echo_fetalobservationfieldmap.observationname = syngo_echo_observationname.name
        and syngo_echo_fetalobservationfieldmap.databasevalue = syngo_echo_observationvalue.val
    where lower(syngo_echo_observationname.name) in ('no_lmp_obs', 'preg_hx_comment_obs', 'type_of_gestation_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'no_lmp_obs' then displayvalue end as no_lmp_obs,
        case when observation_name = 'preg_hx_comment_obs' then displayvalue end as preg_hx_comment_obs,
        case when observation_name = 'type_of_gestation_obs' then displayvalue end as type_of_gestation_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        group_concat(no_lmp_obs, ';') as no_lmp_obs,
        group_concat(preg_hx_comment_obs, ';') as preg_hx_comment_obs,
        group_concat(type_of_gestation_obs, ';') as type_of_gestation_obs
    from observations
    group by studyid
),

measurements as (
    select
        studyid,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'assigned_ma_calc' then floatvalue end) as decimal (27, 12)) as assigned_ma_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'clinical_ma_calc' then floatvalue end) as decimal (27, 12)) as clinical_ma_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'gravida' then integervalue end) as integer) as gravida,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'para' then integervalue end) as integer) as para,
        min(case when lower(syngo_echo_measurementtype.name) = 'assigned_edd_calc' then datetimevalue end) as assigned_edd_calc,
        min(case when lower(syngo_echo_measurementtype.name) = 'lmp_calc' then datetimevalue end) as lmp_calc,
        min(case when lower(syngo_echo_measurementtype.name) = 'clinical_edd_calc' then datetimevalue end) as clinical_edd_calc,
        min(case when lower(syngo_echo_measurementtype.name) = 'ob_anchorstudy_calc' then datetimevalue end) as ob_anchorstudy_calc,
        min(case when lower(syngo_echo_measurementtype.name) = 'assigned_edd_method_calc' then stringvalue end) as assigned_edd_method_calc
    from {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype
        on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    where
        lower(syngo_echo_measurementtype.name) in ('assigned_ma_calc', 'clinical_ma_calc', 'gravida', 'para', 'assigned_edd_calc',
        'clinical_edd_calc', 'lmp_calc', 'ob_anchorstudy_calc', 'assigned_edd_method_calc')
    group by
        studyid
),

sq_echo_fetal_study_pregnancy as (
    select
        fetal_echos.echo_fetal_study_id,
        ob_anchorstudy_calc as anchor_study_date,
        assigned_edd_method_calc as assigned_estimated_delivery_date_method,
        assigned_edd_calc as assigned_estimated_delivery_date,
        assigned_ma_calc as assigned_menstrual_age,
        clinical_edd_calc as clinical_estimated_delivery_date,
        clinical_ma_calc as clinical_menstrual_age,
        gravida as gravida,
        lmp_calc as lmp_date,
        no_lmp_obs as no_lmp_reason,
        para,
        rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(preg_hx_comment_obs as varchar(255)), chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as pregnancy_history_comment,
        type_of_gestation_obs as type_of_gestation
    from fetal_echos
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
    left join measurements
        on fetal_echos.source_system_id = measurements.studyid
    where (assigned_edd_method_calc is not null or assigned_edd_calc is not null or assigned_ma_calc is not null or clinical_edd_calc is not null or clinical_ma_calc is not null or gravida is not null or lmp_calc is not null or no_lmp_obs is not null or ob_anchorstudy_calc is not null or para is not null or preg_hx_comment_obs is not null or type_of_gestation_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(anchor_study_date as timestamp) as anchor_study_date,
    cast(assigned_estimated_delivery_date_method as varchar(50)) as assigned_estimated_delivery_date_method,
    cast(assigned_estimated_delivery_date as timestamp) as assigned_estimated_delivery_date,
    cast(assigned_menstrual_age as numeric(28, 15)) as assigned_menstrual_age,
    cast(clinical_estimated_delivery_date as timestamp) as clinical_estimated_delivery_date,
    cast(clinical_menstrual_age as numeric(28, 15)) as clinical_menstrual_age,
    cast(gravida as integer) as gravida,
    cast(lmp_date as timestamp) as lmp_date,
    cast(no_lmp_reason as varchar(50)) as no_lmp_reason,
    cast(para as integer) as para,
    cast(pregnancy_history_comment as varchar(255)) as pregnancy_history_comment,
    cast(type_of_gestation as varchar(50)) as type_of_gestation
from sq_echo_fetal_study_pregnancy
