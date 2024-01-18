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
        syngo_echo_observationvalue.ownerid,
        lower(syngo_echo_observationname.name) as observation_name,
        isnull(cast(syngo_echo_fetalobservationfieldmap.worksheetvalue as varchar(400)),
            cast(syngo_echo_observationvalue.val as varchar(400))) as displayvalue
    from {{ source('syngo_echo_ods', 'syngo_echo_observationvalue') }} as syngo_echo_observationvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_observationname') }} as syngo_echo_observationname
        on syngo_echo_observationvalue.observationid = syngo_echo_observationname.id
    left join {{ source('syngo_echo_ods', 'syngo_echo_fetalobservationfieldmap') }} as syngo_echo_fetalobservationfieldmap
        on syngo_echo_fetalobservationfieldmap.observationname = syngo_echo_observationname.name
        and syngo_echo_fetalobservationfieldmap.databasevalue = syngo_echo_observationvalue.val
    where lower(syngo_echo_observationname.name) in ('truncus_arteriosus_type_obs', 'd_tga_obs', 'rv_aorta_vsd_size_obs',
        'dolv_type_obs', 'ap_window_obs', 'normal_conotruncal_anatomy_obs', 'tof_a_obs', 'dorv_level_1_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'truncus_arteriosus_type_obs' then displayvalue end as truncus_arteriosus_type_obs,
        case when observation_name = 'd_tga_obs' then displayvalue end as d_tga_obs,
        case when observation_name = 'rv_aorta_vsd_size_obs' then displayvalue end as rv_aorta_vsd_size_obs,
        case when observation_name = 'dolv_type_obs' then displayvalue end as dolv_type_obs,
        case when observation_name = 'ap_window_obs' then displayvalue end as ap_window_obs,
        case when observation_name = 'normal_conotruncal_anatomy_obs' then displayvalue end as normal_conotruncal_anatomy_obs,
        case when observation_name = 'tof_a_obs' then displayvalue end as tof_a_obs,
        case when observation_name = 'dorv_level_1_obs' then displayvalue end as dorv_level_1_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(truncus_arteriosus_type_obs, ';') as truncus_arteriosus_type_obs,
        group_concat(d_tga_obs, ';') as d_tga_obs,
        group_concat(rv_aorta_vsd_size_obs, ';') as rv_aorta_vsd_size_obs,
        group_concat(dolv_type_obs, ';') as dolv_type_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(ap_window_obs, ';') as ap_window_obs,
        group_concat(normal_conotruncal_anatomy_obs, ';') as normal_conotruncal_anatomy_obs,
        group_concat(tof_a_obs, ';') as tof_a_obs,
        group_concat(dorv_level_1_obs, ';') as dorv_level_1_obs
    from observations
    group by studyid, ownerid
),

sq_echo_fetal_study_conotruncus as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        normal_conotruncal_anatomy_obs as conotruncus_normal_conotruncal_anatomy,
        tof_a_obs as conotruncus_tetralogy_of_fallot,
        dorv_level_1_obs as conotruncus_double_outlet_right_ventricle,
        d_tga_obs as conotruncus_d_tga,
        dolv_type_obs as conotruncus_double_outlet_left_ventricle,
        ap_window_obs as conotruncus_aortopulmonary_window,
        truncus_arteriosus_type_obs as conotruncus_truncus_arteriosus_type,
        rv_aorta_vsd_size_obs as conotruncus_rv_aorta_vsd
    from fetal_echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study_owner') }} as syngo_echo_study_owner
        on fetal_echos.source_system_id = syngo_echo_study_owner.study_ref
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
        and syngo_echo_study_owner.ownerid = observation_group_1.ownerid
    left join observation_group_2
        on fetal_echos.source_system_id = observation_group_2.studyid
        and syngo_echo_study_owner.ownerid = observation_group_2.ownerid

where syngo_echo_study_owner.ownertype in (1, 2)
and (ap_window_obs is not null or dolv_type_obs is not null or dorv_level_1_obs is not null or normal_conotruncal_anatomy_obs is not null or rv_aorta_vsd_size_obs is not null or tof_a_obs is not null or truncus_arteriosus_type_obs is not null or d_tga_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(conotruncus_normal_conotruncal_anatomy as varchar(255)) as conotruncus_normal_conotruncal_anatomy,
    cast(conotruncus_tetralogy_of_fallot as varchar(255)) as conotruncus_tetralogy_of_fallot,
    cast(conotruncus_double_outlet_right_ventricle as varchar(255)) as conotruncus_double_outlet_right_ventricle,
    cast(conotruncus_d_tga as varchar(255)) as conotruncus_d_tga,
    cast(conotruncus_double_outlet_left_ventricle as varchar(255)) as conotruncus_double_outlet_left_ventricle,
    cast(conotruncus_aortopulmonary_window as varchar(255)) as conotruncus_aortopulmonary_window,
    cast(conotruncus_truncus_arteriosus_type as varchar(255)) as conotruncus_truncus_arteriosus_type,
    cast(conotruncus_rv_aorta_vsd as varchar(255)) as conotruncus_rv_aorta_vsd
from sq_echo_fetal_study_conotruncus
