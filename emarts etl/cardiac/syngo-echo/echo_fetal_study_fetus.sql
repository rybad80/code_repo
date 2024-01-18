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
    where lower(syngo_echo_observationname.name) in ('placenta_appearance_obs', 'amniotic_fluid_obs', 'hr_and_rhythm_chop_comment_obs',
        'hr_and_rhythm_chop_obs', 'normal_fetal_anatomy_obs', 'umb_cord_chop_hier_obs')
),

observations as (
    select
        studyid,
        ownerid,
case when observation_name = 'placenta_appearance_obs' then displayvalue end as placenta_appearance_obs,
case when observation_name = 'amniotic_fluid_obs' then displayvalue end as amniotic_fluid_obs,
case when observation_name = 'hr_and_rhythm_chop_comment_obs' then displayvalue end as hr_and_rhythm_chop_comment_obs,
case when observation_name = 'hr_and_rhythm_chop_obs' then displayvalue end as hr_and_rhythm_chop_obs,
case when observation_name = 'normal_fetal_anatomy_obs' then displayvalue end as normal_fetal_anatomy_obs,
case when observation_name = 'umb_cord_chop_hier_obs' then displayvalue end as umb_cord_chop_hier_obs
    from observation_display_values
),


observation_group_1 as (
    select
        studyid,
        ownerid,
        group_concat(placenta_appearance_obs, ';') as placenta_appearance_obs,
        group_concat(amniotic_fluid_obs, ';') as amniotic_fluid_obs,
        group_concat(hr_and_rhythm_chop_comment_obs, ';') as hr_and_rhythm_chop_comment_obs,
        group_concat(hr_and_rhythm_chop_obs, ';') as hr_and_rhythm_chop_obs
    from observations
    group by studyid, ownerid
),

observation_group_2 as (
    select
        studyid,
        ownerid,
        group_concat(normal_fetal_anatomy_obs, ';') as normal_fetal_anatomy_obs,
        group_concat(umb_cord_chop_hier_obs, ';') as umb_cord_chop_hier_obs
    from observations
    group by studyid, ownerid
),

measurements as (
    select
        studyid,
        ownerid,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'efw_hadlock_1984_calc' and instancenumber = 0 then floatvalue end) as decimal (27, 12)) as efw_hadlock_1984_calc_avg,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'hc_calc' and instancenumber in(0, 1) then floatvalue end) as decimal (27, 12)) as hc_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bpd_ma_hadlock_1984_calc' and instancenumber = 0 then floatvalue end) as decimal (27, 12)) as bpd_ma_hadlock_1984_calc_avg,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'ac_ma_hadlock_1984_calc' and instancenumber = 0 then floatvalue end) as decimal (27, 12)) as ac_ma_hadlock_1984_calc_avg,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'hc_ma_hadlock_1984_calc' and instancenumber = 0 then floatvalue end) as decimal (27, 12)) as hc_ma_hadlock_1984_calc_avg,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'ac_ma_hadlock_1984_calc' and instancenumber = -3 then floatvalue end) as decimal (27, 12)) as ac_ma_hadlock_1984_calc_sd,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'us_ma_calc' and instancenumber = 0 then floatvalue end) as decimal (27, 12)) as us_ma_calc_avg,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'efw_hadlock_1984_calc' and instancenumber = -3 then floatvalue end) as decimal (27, 12)) as efw_hadlock_1984_calc_sd,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'fl_ma_hadlock_1984_calc' and instancenumber = 0 then floatvalue end) as decimal (27, 12)) as fl_ma_hadlock_1984_calc_avg,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'hc_ma_hadlock_1984_calc' and instancenumber = -3 then floatvalue end) as decimal (27, 12)) as hc_ma_hadlock_1984_calc_sd,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bpd_calc' and instancenumber in(0, 1) then floatvalue end) as decimal (27, 12)) as bpd_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'fl_calc' and instancenumber in(0, 1) then floatvalue end) as decimal (27, 12)) as fl_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bpd_ma_hadlock_1984_calc' and instancenumber = -3 then floatvalue end) as decimal (27, 12)) as bpd_ma_hadlock_1984_calc_sd,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'us_ma_calc' and instancenumber = -3 then floatvalue end) as decimal (27, 12)) as us_ma_calc_sd,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'fl_ma_hadlock_1984_calc' and instancenumber = -3 then floatvalue end) as decimal (27, 12)) as fl_ma_hadlock_1984_calc_sd,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'ac_calc' and instancenumber in(0, 1) then floatvalue end) as decimal (27, 12)) as ac_calc
    from {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype
        on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    where
        lower(syngo_echo_measurementtype.name) in ('ac_ma_hadlock_1984_calc', 'ac_calc', 'bpd_ma_hadlock_1984_calc', 'bpd_calc',
            'efw_hadlock_1984_calc', 'fl_ma_hadlock_1984_calc', 'fl_calc', 'hc_ma_hadlock_1984_calc', 'hc_calc', 'us_ma_calc', 'us_ma_calc')


    group by
        studyid,
        ownerid
),

sq_echo_fetal_study_fetus as (
    select
        fetal_echos.echo_fetal_study_id,
        syngo_echo_study_owner.ownerid as owner_id,
ac_calc as abdominal_circumference_avg,
ac_ma_hadlock_1984_calc_avg as abdominal_circumference_ma_avg,
ac_ma_hadlock_1984_calc_sd as abdominal_circumference_ma_sd,
rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(amniotic_fluid_obs as varchar(50)), chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as amniotic_fluid,
bpd_calc as biparietal_diameter_avg,
bpd_ma_hadlock_1984_calc_avg as biparietal_diameter_ma_avg,
bpd_ma_hadlock_1984_calc_sd as biparietal_diameter_ma_sd,
us_ma_calc_avg as composite_ma_avg,
us_ma_calc_sd as composite_ma_sd,
efw_hadlock_1984_calc_avg as estimated_fetal_weight_avg,
efw_hadlock_1984_calc_sd as estimated_fetal_weight_ma_sd,
fl_calc as femur_length_avg,
fl_ma_hadlock_1984_calc_avg as femur_length_ma_avg,
fl_ma_hadlock_1984_calc_sd as femur_length_ma_sd,
hc_calc as head_circumference_avg,
hc_ma_hadlock_1984_calc_avg as head_circumference_ma_avg,
hc_ma_hadlock_1984_calc_sd as head_circumference_ma_sd,
rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(hr_and_rhythm_chop_comment_obs as varchar(255)), chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as heart_rate_rhythm_comment,
rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(hr_and_rhythm_chop_obs as varchar(50)), chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as heart_rate_rhythm,
rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(normal_fetal_anatomy_obs as varchar(50)), chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as normal_fetal_anatomy,
rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(placenta_appearance_obs as varchar(50)), chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as placenta_appearance,
rtrim(ltrim(replace(replace(replace(replace(replace(replace(cast(umb_cord_chop_hier_obs as varchar(100)), chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '))) as normal_umbilical_cord_anatomy
from fetal_echos
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study_owner') }} as syngo_echo_study_owner
        on fetal_echos.source_system_id = syngo_echo_study_owner.study_ref
    left join observation_group_1
        on fetal_echos.source_system_id = observation_group_1.studyid
        and syngo_echo_study_owner.ownerid = observation_group_1.ownerid
    left join observation_group_2
        on fetal_echos.source_system_id = observation_group_2.studyid
        and syngo_echo_study_owner.ownerid = observation_group_2.ownerid
    left join measurements
        on fetal_echos.source_system_id = measurements.studyid
        and syngo_echo_study_owner.ownerid = measurements.ownerid
where ownertype in (1, 2)
    and (ac_ma_hadlock_1984_calc_avg is not null or ac_ma_hadlock_1984_calc_sd is not null or ac_calc is not null or amniotic_fluid_obs is not null or bpd_ma_hadlock_1984_calc_avg is not null or bpd_ma_hadlock_1984_calc_sd is not null or bpd_calc is not null or efw_hadlock_1984_calc_avg is not null or efw_hadlock_1984_calc_sd is not null or fl_ma_hadlock_1984_calc_avg is not null or fl_ma_hadlock_1984_calc_sd is not null or fl_calc is not null or hc_ma_hadlock_1984_calc_avg is not null or hc_ma_hadlock_1984_calc_sd is not null or hc_calc is not null or hr_and_rhythm_chop_comment_obs is not null or hr_and_rhythm_chop_obs is not null or normal_fetal_anatomy_obs is not null or placenta_appearance_obs is not null or us_ma_calc_avg is not null or us_ma_calc_sd is not null or umb_cord_chop_hier_obs is not null)
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(owner_id as integer) as owner_id,
    cast(abdominal_circumference_avg as numeric(28, 15)) as abdominal_circumference_avg,
    cast(abdominal_circumference_ma_avg as numeric(28, 15)) as abdominal_circumference_ma_avg,
    cast(abdominal_circumference_ma_sd as numeric(28, 15)) as abdominal_circumference_ma_sd,
    cast(amniotic_fluid as varchar(50)) as amniotic_fluid,
    cast(biparietal_diameter_avg as numeric(28, 15)) as biparietal_diameter_avg,
    cast(biparietal_diameter_ma_avg as numeric(28, 15)) as biparietal_diameter_ma_avg,
    cast(biparietal_diameter_ma_sd as numeric(28, 15)) as biparietal_diameter_ma_sd,
    cast(composite_ma_avg as numeric(28, 15)) as composite_ma_avg,
    cast(composite_ma_sd as numeric(28, 15)) as composite_ma_sd,
    cast(estimated_fetal_weight_avg as numeric(28, 15)) as estimated_fetal_weight_avg,
    cast(estimated_fetal_weight_ma_sd as numeric(28, 15)) as estimated_fetal_weight_ma_sd,
    cast(femur_length_avg as numeric(28, 15)) as femur_length_avg,
    cast(femur_length_ma_avg as numeric(28, 15)) as femur_length_ma_avg,
    cast(femur_length_ma_sd as numeric(28, 15)) as femur_length_ma_sd,
    cast(head_circumference_avg as numeric(28, 15)) as head_circumference_avg,
    cast(head_circumference_ma_avg as numeric(28, 15)) as head_circumference_ma_avg,
    cast(head_circumference_ma_sd as numeric(28, 15)) as head_circumference_ma_sd,
    cast(heart_rate_rhythm_comment as varchar(255)) as heart_rate_rhythm_comment,
    cast(heart_rate_rhythm as varchar(50)) as heart_rate_rhythm,
    cast(normal_fetal_anatomy as varchar(255)) as normal_fetal_anatomy,
    cast(placenta_appearance as varchar(255)) as placenta_appearance,
    cast(normal_umbilical_cord_anatomy as varchar(255)) as normal_umbilical_cord_anatomy
from sq_echo_fetal_study_fetus
