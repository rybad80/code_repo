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
        rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(val as varchar(3000)), chr(9), ' '),
            chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as displayvalue
    from {{ source('syngo_echo_ods', 'syngo_echo_observationvalue') }} as syngo_echo_observationvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_observationname') }} as syngo_echo_observationname
       on syngo_echo_observationvalue.observationid = syngo_echo_observationname.id
    where lower(syngo_echo_observationname.name) in ('pab_ra_comments_80818_obs', 'pab_la_comments_80818_obs',
    'chw_asd_atrial_septum_comments_80818_obs', 'chw_tv_comments_obs', 'chw_mv_comments_obs', 'av_canal_comments_obs',
    'chw_lv_comments_obs', 'chw_rvot_comments_obs', 'chw_pv_comments_obs', 'chw_lvot_comments_obs',
    'chw_aov_comments_1_obs', 'chw_ao_comments_obs', 'chw_pa_comments_obs', 'chw_ca_comments_obs', 'chw_pda_comments_1_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'pab_ra_comments_80818_obs' then displayvalue end as right_atrium_comment,
        case when observation_name = 'pab_la_comments_80818_obs' then displayvalue end as left_atrium_comment,
        case when observation_name = 'chw_asd_atrial_septum_comments_80818_obs' then displayvalue end as atrial_septum_comment,
        case when observation_name = 'chw_tv_comments_obs' then displayvalue end as tricuspid_valve_comment,
        case when observation_name = 'chw_mv_comments_obs' then displayvalue end as mitral_valve_comment,
        case when observation_name = 'av_canal_comments_obs' then displayvalue end as avcanal_comment,
        case when observation_name = 'chw_lv_comments_obs' then displayvalue end as left_ventricle_comment,
        case when observation_name = 'chw_rvot_comments_obs' then displayvalue end as rvot_comment,
        case when observation_name = 'chw_pv_comments_obs' then displayvalue end as pulmonary_valve_comment,
        case when observation_name = 'chw_lvot_comments_obs' then displayvalue end as lvot_comment,
        case when observation_name = 'chw_aov_comments_1_obs' then displayvalue end as aortic_valve_comment,
        case when observation_name = 'chw_ao_comments_obs' then displayvalue end as aorta_comment,
        case when observation_name = 'chw_pa_comments_obs' then displayvalue end as pulmonary_arteries_comment,
        case when observation_name = 'chw_ca_comments_obs' then displayvalue end as coronary_arteries_comment,
        case when observation_name = 'chw_pda_comments_1_obs' then displayvalue end as pda_comment
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        group_concat(right_atrium_comment, ';') as right_atrium_comment,
        group_concat(left_atrium_comment, ';') as left_atrium_comment,
        group_concat(atrial_septum_comment, ';') as atrial_septum_comment,
        group_concat(tricuspid_valve_comment, ';') as tricuspid_valve_comment
    from observations
    group by studyid
),

observation_group_2 as (
    select
        studyid,
        group_concat(mitral_valve_comment, ';') as mitral_valve_comment,
        group_concat(avcanal_comment, ';') as avcanal_comment,
        group_concat(left_ventricle_comment, ';') as left_ventricle_comment,
        group_concat(rvot_comment, ';') as rvot_comment
    from observations
    group by studyid
),

observation_group_3 as (
    select
        studyid,
        group_concat(pulmonary_valve_comment, ';') as pulmonary_valve_comment,
        group_concat(lvot_comment, ';') as lvot_comment,
        group_concat(aortic_valve_comment, ';') as aortic_valve_comment,
        group_concat(aorta_comment, ';') as aorta_comment
    from observations
    group by studyid
),

observation_group_4 as (
    select
        studyid,
        group_concat(pulmonary_arteries_comment, ';') as pulmonary_arteries_comment,
        group_concat(coronary_arteries_comment, ';') as coronary_arteries_comment,
        group_concat(pda_comment, ';') as pda_comment
    from observations
    group by studyid
),

sq_echo_study_comments as (
    select
        echo_study_id,
        nullif(right_atrium_comment, '') as right_atrium_comment,
        nullif(left_atrium_comment, '') as left_atrium_comment,
        nullif(atrial_septum_comment, '') as atrial_septum_comment,
        nullif(tricuspid_valve_comment, '') as tricuspid_valve_comment,
        nullif(mitral_valve_comment, '') as mitral_valve_comment,
        nullif(avcanal_comment, '') as avcanal_comment,
        nullif(left_ventricle_comment, '') as left_ventricle_comment,
        nullif(rvot_comment, '') as rvot_comment,
        nullif(pulmonary_valve_comment, '') as pulmonary_valve_comment,
        nullif(lvot_comment, '') as lvot_comment,
        nullif(aortic_valve_comment, '') as aortic_valve_comment,
        nullif(aorta_comment, '') as aorta_comment,
        nullif(pulmonary_arteries_comment, '') as pulmonary_arteries_comment,
        nullif(coronary_arteries_comment, '') as coronary_arteries_comment,
        nullif(pda_comment, '') as pda_comment
    from echos
    left join observation_group_1
        on echos.source_system_id = observation_group_1.studyid
    left join observation_group_2
        on echos.source_system_id = observation_group_2.studyid
    left join observation_group_3
        on echos.source_system_id = observation_group_3.studyid
    left join observation_group_4
        on echos.source_system_id = observation_group_4.studyid
    where
        (right_atrium_comment is not null or left_atrium_comment is not null or atrial_septum_comment is not null
            or tricuspid_valve_comment is not null or mitral_valve_comment is not null or avcanal_comment is not null
            or left_ventricle_comment is not null or rvot_comment is not null or pulmonary_valve_comment is not null
            or lvot_comment is not null or aortic_valve_comment is not null or aorta_comment is not null
            or pulmonary_arteries_comment is not null or coronary_arteries_comment is not null or pda_comment is not null)
)

select
    cast(echo_study_id as varchar(25)) as echo_study_id,
    cast(right_atrium_comment as varchar(3000)) as right_atrium_comment,
    cast(left_atrium_comment as varchar(3000)) as left_atrium_comment,
    cast(atrial_septum_comment as varchar(3000)) as atrial_septum_comment,
    cast(tricuspid_valve_comment as varchar(3000)) as tricuspid_valve_comment,
    cast(mitral_valve_comment as varchar(3000)) as mitral_valve_comment,
    cast(avcanal_comment as varchar(3000)) as avcanal_comment,
    cast(left_ventricle_comment as varchar(3000)) as left_ventricle_comment,
    cast(rvot_comment as varchar(3000)) as rvot_comment,
    cast(pulmonary_valve_comment as varchar(3000)) as pulmonary_valve_comment,
    cast(lvot_comment as varchar(3000)) as lvot_comment,
    cast(aortic_valve_comment as varchar(3000)) as aortic_valve_comment,
    cast(aorta_comment as varchar(3000)) as aorta_comment,
    cast(pulmonary_arteries_comment as varchar(3000)) as pulmonary_arteries_comment,
    cast(coronary_arteries_comment as varchar(3000)) as coronary_arteries_comment,
    cast(pda_comment as varchar(3000)) as pda_comment
from sq_echo_study_comments
