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
'mv_inflow_obs', 'pulm_insuff_obs', 'twin_twin_comment_obs', 'vent_hyper_obs',
'umbil_art_obs', 'card_enlarge_obs', 'umbil_vn_obs', 'mitral_regurg_obs',
'tv_inflow_obs', 'ductus_ven_obs', 'outflow_tracts_obs', 'tricusp_reg_obs', 'syst_dysfx_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'mv_inflow_obs' then displayvalue end as mv_inflow_obs,
        case when observation_name = 'pulm_insuff_obs' then displayvalue end as pulm_insuff_obs,
        case when observation_name = 'twin_twin_comment_obs' then displayvalue end as twin_twin_comment_obs,
        case when observation_name = 'vent_hyper_obs' then displayvalue end as vent_hyper_obs,
        case when observation_name = 'umbil_art_obs' then displayvalue end as umbil_art_obs,
        case when observation_name = 'card_enlarge_obs' then displayvalue end as card_enlarge_obs,
        case when observation_name = 'umbil_vn_obs' then displayvalue end as umbil_vn_obs,
        case when observation_name = 'mitral_regurg_obs' then displayvalue end as mitral_regurg_obs,
        case when observation_name = 'tv_inflow_obs' then displayvalue end as tv_inflow_obs,
        case when observation_name = 'ductus_ven_obs' then displayvalue end as ductus_ven_obs,
        case when observation_name = 'outflow_tracts_obs' then displayvalue end as outflow_tracts_obs,
        case when observation_name = 'tricusp_reg_obs' then displayvalue end as tricusp_reg_obs,
        case when observation_name = 'syst_dysfx_obs' then displayvalue end as syst_dysfx_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
group_concat(mv_inflow_obs, ';') as mv_inflow_obs,
group_concat(pulm_insuff_obs, ';') as pulm_insuff_obs,
group_concat(twin_twin_comment_obs, ';') as twin_twin_comment_obs,
group_concat(vent_hyper_obs, ';') as vent_hyper_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(umbil_art_obs, ';') as umbil_art_obs,
        group_concat(card_enlarge_obs, ';') as card_enlarge_obs,
        group_concat(umbil_vn_obs, ';') as umbil_vn_obs,
        group_concat(mitral_regurg_obs, ';') as mitral_regurg_obs
    from observations
    group by studyid, ownerid
),

observation_group_3 as (
    select
        studyid,
        ownerid,
        group_concat(tv_inflow_obs, ';') as tv_inflow_obs,
        group_concat(ductus_ven_obs, ';') as ductus_ven_obs,
        group_concat(outflow_tracts_obs, ';') as outflow_tracts_obs,
        group_concat(tricusp_reg_obs, ';') as tricusp_reg_obs
    from observations
    group by studyid, ownerid
),

observation_group_4 as (
    select
        studyid,
        ownerid,
        group_concat(syst_dysfx_obs, ';') as syst_dysfx_obs
    from observations
    group by studyid, ownerid
),

measurements as (
    select
        studyid,
        ownerid,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'twn_twn_total_score_calc' then floatvalue end) as decimal (27, 12)) as twn_twn_total_score_calc
    from {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype
        on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    where
        lower(syngo_echo_measurementtype.name) in ('twn_twn_total_score_calc')
    group by
        studyid,
        ownerid
),

sq_echo_fetal_study_cardiovasc as (
    select
        cast(syngo_echo_study_owner.study_ref as varchar(25)) || 'Syn' as echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        umbil_art_obs as cardiovasc_umbilical_artery,
        card_enlarge_obs as cardiovasc_cardiac_enlargement,
        syst_dysfx_obs as cardiovasc_systolic_dysfunction,
        vent_hyper_obs as cardiovasc_ventricular_hypertrophy,
        tricusp_reg_obs as cardiovasc_tricuspid_regurgitation,
        mitral_regurg_obs as cardiovasc_mitral_regurgitation,
        tv_inflow_obs as cardiovasc_tv_inflow,
        mv_inflow_obs as cardiovasc_mv_inflow,
        ductus_ven_obs as cardiovasc_ductus_venosus,
        umbil_vn_obs as cardiovasc_umbilical_vein,
        outflow_tracts_obs as cardiovasc_outflow_tracts,
        pulm_insuff_obs as cardiovasc_pulmonary_insufficiency,
        twn_twn_total_score_calc as cardiovasc_total_score_avg,
        rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(twin_twin_comment_obs as varchar(255)), chr(9), ' '),
            chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as cardiovasc_twin_twin_comment
    from fetal_echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study_owner') }} as syngo_echo_study_owner
        on fetal_echos.source_system_id = syngo_echo_study_owner.study_ref
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
        and syngo_echo_study_owner.ownerid = observation_group_1.ownerid
    left join observation_group_2
        on fetal_echos.source_system_id = observation_group_2.studyid
        and syngo_echo_study_owner.ownerid = observation_group_2.ownerid
    left join observation_group_3
        on fetal_echos.source_system_id = observation_group_3.studyid
        and syngo_echo_study_owner.ownerid = observation_group_3.ownerid
    left join observation_group_4
        on fetal_echos.source_system_id = observation_group_4.studyid
        and syngo_echo_study_owner.ownerid = observation_group_4.ownerid
    left join measurements
        on fetal_echos.source_system_id = measurements.studyid
        and syngo_echo_study_owner.ownerid = measurements.ownerid
where
syngo_echo_study_owner.ownertype in (1, 2)
and (card_enlarge_obs is not null or ductus_ven_obs is not null or mv_inflow_obs is not null or mitral_regurg_obs is not null or outflow_tracts_obs is not null or pulm_insuff_obs is not null or syst_dysfx_obs is not null or tv_inflow_obs is not null or tricusp_reg_obs is not null or twin_twin_comment_obs is not null or twn_twn_total_score_calc is not null or umbil_art_obs is not null or umbil_vn_obs is not null or vent_hyper_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(cardiovasc_umbilical_artery as varchar(100)) as cardiovasc_umbilical_artery,
    cast(cardiovasc_cardiac_enlargement as varchar(100)) as cardiovasc_cardiac_enlargement,
    cast(cardiovasc_systolic_dysfunction as varchar(100)) as cardiovasc_systolic_dysfunction,
    cast(cardiovasc_ventricular_hypertrophy as varchar(100)) as cardiovasc_ventricular_hypertrophy,
    cast(cardiovasc_tricuspid_regurgitation as varchar(100)) as cardiovasc_tricuspid_regurgitation,
    cast(cardiovasc_mitral_regurgitation as varchar(100)) as cardiovasc_mitral_regurgitation,
    cast(cardiovasc_tv_inflow as varchar(100)) as cardiovasc_tv_inflow,
    cast(cardiovasc_mv_inflow as varchar(100)) as cardiovasc_mv_inflow,
    cast(cardiovasc_ductus_venosus as varchar(100)) as cardiovasc_ductus_venosus,
    cast(cardiovasc_umbilical_vein as varchar(100)) as cardiovasc_umbilical_vein,
    cast(cardiovasc_outflow_tracts as varchar(100)) as cardiovasc_outflow_tracts,
    cast(cardiovasc_pulmonary_insufficiency as varchar(100)) as cardiovasc_pulmonary_insufficiency,
    cast(cardiovasc_total_score_avg as numeric(28, 15)) as cardiovasc_total_score_avg,
    cast(cardiovasc_twin_twin_comment as varchar(500)) as cardiovasc_twin_twin_comment
from sq_echo_fetal_study_cardiovasc
