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
    where lower(syngo_echo_observationname.name) in ('rpa_dx_structure_severity_obs', 'aortic_arch_dx_severity_obs',
    'coarctation_type_severity_chop_obs', 'lpa_dx_structure_severity_obs', 'mpa_dx_structure_severity_obs',
    'chw_pa_branches_normal_summary_obs', 'chw_pa_normal_main_and_branch_summary_obs', 'arch_sidedness_obs',
    'pda_type_size_chop_0_obs', 'transverse_arch_hypoplasia_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'rpa_dx_structure_severity_obs' then displayvalue end as rpa_dx_structure_severity_obs,
        case when observation_name = 'aortic_arch_dx_severity_obs' then displayvalue end as aortic_arch_dx_severity_obs,
        case when observation_name = 'coarctation_type_severity_chop_obs' then displayvalue end as coarctation_type_severity_chop_obs,
        case when observation_name = 'lpa_dx_structure_severity_obs' then displayvalue end as lpa_dx_structure_severity_obs,
        case when observation_name = 'mpa_dx_structure_severity_obs' then displayvalue end as mpa_dx_structure_severity_obs,
        case when observation_name = 'chw_pa_branches_normal_summary_obs' then displayvalue end as chw_pa_branches_normal_summary_obs,
        case when observation_name = 'chw_pa_normal_main_and_branch_summary_obs' then displayvalue end as chw_pa_normal_main_and_branch_summary_obs,
        case when observation_name = 'arch_sidedness_obs' then displayvalue end as arch_sidedness_obs,
        case when observation_name = 'pda_type_size_chop_0_obs' then displayvalue end as pda_type_size_chop_0_obs,
        case when observation_name = 'transverse_arch_hypoplasia_obs' then displayvalue end as transverse_arch_hypoplasia_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(rpa_dx_structure_severity_obs, ';') as rpa_dx_structure_severity_obs,
        group_concat(aortic_arch_dx_severity_obs, ';') as aortic_arch_dx_severity_obs,
        group_concat(coarctation_type_severity_chop_obs, ';') as coarctation_type_severity_chop_obs,
        group_concat(lpa_dx_structure_severity_obs, ';') as lpa_dx_structure_severity_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(mpa_dx_structure_severity_obs, ';') as mpa_dx_structure_severity_obs,
        group_concat(chw_pa_branches_normal_summary_obs, ';') as chw_pa_branches_normal_summary_obs,
        group_concat(chw_pa_normal_main_and_branch_summary_obs, ';') as chw_pa_normal_main_and_branch_summary_obs,
        group_concat(arch_sidedness_obs, ';') as arch_sidedness_obs
    from observations
    group by studyid, ownerid
),

observation_group_3 as (
    select
        studyid,
        ownerid,
        group_concat(pda_type_size_chop_0_obs, ';') as pda_type_size_chop_0_obs,
        group_concat(transverse_arch_hypoplasia_obs, ';') as transverse_arch_hypoplasia_obs
    from observations
    group by studyid, ownerid
),

sq_echo_fetal_study_arteries as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        aortic_arch_dx_severity_obs as aorta_arch_diagnosis_severity,
        arch_sidedness_obs as aorta_arch_sidedness,
        coarctation_type_severity_chop_obs as aorta_coarct_type_severity,
        transverse_arch_hypoplasia_obs as aorta_trans_arch_hypoplasia,
        pda_type_size_chop_0_obs as pda_type_size,
        chw_pa_normal_main_and_branch_summary_obs as pulmonary_arteries_normal,
        chw_pa_branches_normal_summary_obs as pulmonary_arteries_branches_normal,
        lpa_dx_structure_severity_obs as pulmonary_arteries_lpa_structure_severity,
        mpa_dx_structure_severity_obs as pulmonary_arteries_mpa_structure_severity,
        rpa_dx_structure_severity_obs as pulmonary_arteries_rpa_structure_severity
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
    where
        syngo_echo_study_owner.ownertype in (1, 2)
        and (aortic_arch_dx_severity_obs is not null or arch_sidedness_obs is not null or coarctation_type_severity_chop_obs is not null
        or lpa_dx_structure_severity_obs is not null or mpa_dx_structure_severity_obs is not null or pda_type_size_chop_0_obs is not null
        or rpa_dx_structure_severity_obs is not null or transverse_arch_hypoplasia_obs is not null
        or chw_pa_branches_normal_summary_obs is not null or chw_pa_normal_main_and_branch_summary_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(aorta_arch_diagnosis_severity as varchar(255)) as aorta_arch_diagnosis_severity,
    cast(aorta_arch_sidedness as varchar(255)) as aorta_arch_sidedness,
    cast(aorta_coarct_type_severity as varchar(255)) as aorta_coarct_type_severity,
    cast(aorta_trans_arch_hypoplasia as varchar(255)) as aorta_trans_arch_hypoplasia,
    cast(pda_type_size as varchar(255)) as pda_type_size,
    cast(pulmonary_arteries_normal as varchar(255)) as pulmonary_arteries_normal,
    cast(pulmonary_arteries_branches_normal as varchar(255)) as pulmonary_arteries_branches_normal,
    cast(pulmonary_arteries_lpa_structure_severity as varchar(255)) as pulmonary_arteries_lpa_structure_severity,
    cast(pulmonary_arteries_mpa_structure_severity as varchar(255)) as pulmonary_arteries_mpa_structure_severity,
    cast(pulmonary_arteries_rpa_structure_severity as varchar(255)) as pulmonary_arteries_rpa_structure_severity
from sq_echo_fetal_study_arteries
