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
    where lower(syngo_echo_observationname.name) in (
    'pab_asd_location_site_size_obs', 'chw_ra_size_obs', 'chw_la_size_obs', 'chw_asd_pfo_obs', 'chop_asd_atrial_septum_description_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'pab_asd_location_site_size_obs' then displayvalue end as pab_asd_location_site_size_obs,
        case when observation_name = 'chw_ra_size_obs' then displayvalue end as chw_ra_size_obs,
        case when observation_name = 'chw_la_size_obs' then displayvalue end as chw_la_size_obs,
        case when observation_name = 'chw_asd_pfo_obs' then displayvalue end as chw_asd_pfo_obs,
        case when observation_name = 'chop_asd_atrial_septum_description_obs' then displayvalue end as chop_asd_atrial_septum_description_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(pab_asd_location_site_size_obs, ';') as pab_asd_location_site_size_obs,
        group_concat(chw_ra_size_obs, ';') as chw_ra_size_obs,
        group_concat(chw_la_size_obs, ';') as chw_la_size_obs,
        group_concat(chw_asd_pfo_obs, ';') as chw_asd_pfo_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
group_concat(chop_asd_atrial_septum_description_obs, ';') as chop_asd_atrial_septum_description_obs
    from observations
    group by studyid, ownerid
),

sq_echo_fetal_study_atria as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        pab_asd_location_site_size_obs as atrial_septal_defect_type_size,
        chop_asd_atrial_septum_description_obs as atrial_septum,
        chw_la_size_obs as left_atrium_size,
        chw_asd_pfo_obs as patent_foramen_ovale,
        chw_ra_size_obs as right_atrium_size
    from fetal_echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study_owner') }} as syngo_echo_study_owner
        on fetal_echos.source_system_id = syngo_echo_study_owner.study_ref
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
        and syngo_echo_study_owner.ownerid = observation_group_1.ownerid
    left join observation_group_2
        on fetal_echos.source_system_id = observation_group_2.studyid
        and syngo_echo_study_owner.ownerid = observation_group_2.ownerid
where
syngo_echo_study_owner.ownertype in (1, 2)
and (chop_asd_atrial_septum_description_obs is not null or pab_asd_location_site_size_obs is not null or chw_asd_pfo_obs is not null or chw_la_size_obs is not null or chw_ra_size_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(atrial_septal_defect_type_size as varchar(255)) as atrial_septal_defect_type_size,
    cast(atrial_septum as varchar(255)) as atrial_septum,
    cast(left_atrium_size as varchar(255)) as left_atrium_size,
    cast(patent_foramen_ovale as varchar(255)) as patent_foramen_ovale,
    cast(right_atrium_size as varchar(255)) as right_atrium_size
from sq_echo_fetal_study_atria
