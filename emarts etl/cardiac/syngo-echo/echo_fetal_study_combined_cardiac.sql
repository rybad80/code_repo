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
    where lower(syngo_echo_observationname.name) in('combined_card_output_comment_obs')
),

observations as (
    select
        studyid,
        ownerid,
        case when observation_name = 'combined_card_output_comment_obs' then displayvalue end as combined_card_output_comment_obs
    from observation_display_values
),

observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(combined_card_output_comment_obs, ';') as combined_card_output_comment_obs
    from observations
    group by studyid, ownerid
),

measurements as (
    select
        studyid,
        ownerid,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'ao_ann_diam_s_plax_calc' then floatvalue end) as decimal (28, 15)) as ao_ann_diam_s_plax_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'efw_manual_entry_calc' then floatvalue end) as decimal (28, 15)) as efw_manual_entry_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'aov_vti_calc' then floatvalue end) as decimal (28, 15)) as aov_vti_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'pv_vti_calc' then floatvalue end) as decimal (28, 15)) as pv_vti_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'left_cardiac_output_0_calc' then floatvalue end) as decimal (28, 15)) as left_cardiac_output_0_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'right_cardiac_output_0_calc' then floatvalue end) as decimal (28, 15)) as right_cardiac_output_0_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'combined_cardiac_output_0_calc' then floatvalue end) as decimal (28, 15)) as combined_cardiac_output_0_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'fetal_hr_1_calc' then floatvalue end) as decimal (28, 15)) as fetal_hr_1_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'pv_ann_diam_s_psax_calc' then floatvalue end) as decimal (28, 15)) as pv_ann_diam_s_psax_calc
    from {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype
        on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    where
        lower(syngo_echo_measurementtype.name) in ('aov_vti_calc', 'ao_ann_diam_s_plax_calc', 'combined_cardiac_output_0_calc',
            'efw_manual_entry_calc', 'fetal_hr_1_calc', 'left_cardiac_output_0_calc', 'pv_ann_diam_s_psax_calc', 'pv_vti_calc',
            'right_cardiac_output_0_calc')
    group by
        studyid,
        ownerid
),

sq_echo_fetal_study_combined_cardiac as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
        ao_ann_diam_s_plax_calc as left_annulus_avg,
        aov_vti_calc as left_velocity_time_integral_avg,
        fetal_hr_1_calc as heart_rate_avg,
        efw_manual_entry_calc as estimated_fetal_weight_avg,
        left_cardiac_output_0_calc as left_cardiac_output_avg,
        pv_ann_diam_s_psax_calc as right_annulus_avg,
        pv_vti_calc as right_velocity_time_integral_avg,
        right_cardiac_output_0_calc as right_cardiac_output_avg,
        combined_cardiac_output_0_calc as combined_cardiac_output_avg,
        rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(combined_card_output_comment_obs as varchar(255)), chr(9), ' '),
            chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as combined_cardiac_output_comment
    from fetal_echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study_owner') }} as syngo_echo_study_owner
        on fetal_echos.source_system_id = syngo_echo_study_owner.study_ref
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
        and syngo_echo_study_owner.ownerid = observation_group_1.ownerid
    left join measurements
        on fetal_echos.source_system_id = measurements.studyid
        and syngo_echo_study_owner.ownerid = measurements.ownerid
    where
    syngo_echo_study_owner.ownertype in (1, 2)
        and (aov_vti_calc is not null or ao_ann_diam_s_plax_calc is not null or combined_card_output_comment_obs is not null or combined_cardiac_output_0_calc is not null or efw_manual_entry_calc is not null or fetal_hr_1_calc is not null or left_cardiac_output_0_calc is not null or pv_ann_diam_s_psax_calc is not null or pv_vti_calc is not null or right_cardiac_output_0_calc is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(left_annulus_avg as numeric(28, 15)) as left_annulus_avg,
    cast(left_velocity_time_integral_avg as numeric(28, 15)) as left_velocity_time_integral_avg,
    cast(heart_rate_avg as numeric(28, 15)) as heart_rate_avg,
    cast(estimated_fetal_weight_avg as numeric(28, 15)) as estimated_fetal_weight_avg,
    cast(left_cardiac_output_avg as numeric(28, 15)) as left_cardiac_output_avg,
    cast(right_annulus_avg as numeric(28, 15)) as right_annulus_avg,
    cast(right_velocity_time_integral_avg as numeric(28, 15)) as right_velocity_time_integral_avg,
    cast(right_cardiac_output_avg as numeric(28, 15)) as right_cardiac_output_avg,
    cast(combined_cardiac_output_avg as numeric(28, 15)) as combined_cardiac_output_avg,
    cast(combined_cardiac_output_comment as varchar(255)) as combined_cardiac_output_comment
from sq_echo_fetal_study_combined_cardiac
