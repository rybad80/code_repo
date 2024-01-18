with flowsheets_per_timestamp as (
    --region subcomponents of Violent Restraint Justification and Q15 Assessment flowsheets
    --consider device type as restraint removal time varies
    select
        stg_restraints.restraint_episode_key,
        stg_restraints_flowsheets_raw.recorded_date,
        row_number() over(
            partition by
                stg_restraints.restraint_episode_key
            order by
                stg_restraints_flowsheets_raw.recorded_date
        ) as flowsheet_number,
        /*Violent Restraint Justification*/
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071738 --Precipitating Factors
                then 1 else 0 end) as fs_factors_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071737 --Less Restrictive Alternative
                then 1 else 0 end) as fs_alternative_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071740 --Clinical Justification
                then 1 else 0 end) as fs_justification_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071756 --Restraint Types
        /*Visual Observation*/
                then 1 else 0 end) as fs_device_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071747 --Visual Observation
                then 1 else 0 end) as fs_visual_observation_ind,
        (fs_factors_ind
            + fs_alternative_ind
            + fs_justification_ind
            + fs_device_ind
            + fs_visual_observation_ind) / 5 as fs_complete_ind,
        /*Upon Starting AND Q15 Minute RN Assessments Only*/
        lead(stg_restraints_flowsheets_raw.recorded_date) over (
            partition by
                stg_restraints.restraint_episode_key
            order by
                stg_restraints_flowsheets_raw.recorded_date
        ) as next_recorded_date,
        minutes_between(next_recorded_date, stg_restraints_flowsheets_raw.recorded_date) as q15_time_diff,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071746 --Circulation/Skin Integrity (WDL)
            then 1 else 0 end) as q15_circulation_ind,
        --Behavior Warranting Restraints Continued
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40068161
            then 1 else 0 end) as q15_behavior_check_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071744 --Observable Patient Behaviors
            then 1 else 0 end) as q15_obs_behavior_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071745 --Physical Comfort/Device check
            then 1 else 0 end) as q15_device_check_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 8 --Pulse
            then 1 else 0 end) as q15_pulse_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 9 --Resp
            then 1 else 0 end) as q15_respiratory_ind,
        case --no Q15 necessary if restraint < 15 minutes
            when stg_restraints.restraint_duration_hours <= 0.25
                then -1
            --upon removal, behavior warranting restraint continued should be completed
            when stg_restraints_flowsheets_raw.recorded_date = stg_restraints.restraint_removal
                then q15_behavior_check_ind
            else (q15_circulation_ind
                + q15_behavior_check_ind
                + q15_obs_behavior_ind
                + q15_device_check_ind
                + q15_device_check_ind
                + q15_respiratory_ind) / 6  end as q15_rows_complete_ind
    from
        {{ ref('stg_restraints') }} as stg_restraints
        left join {{ ref('stg_restraints_flowsheets_raw') }} as stg_restraints_flowsheets_raw
            on stg_restraints.visit_key = stg_restraints_flowsheets_raw.visit_key
            and stg_restraints_flowsheets_raw.recorded_date between
                stg_restraints.restraint_start and coalesce(stg_restraints.restraint_removal, current_date)
    group by
        stg_restraints.restraint_episode_key,
        stg_restraints.restraint_removal,
        stg_restraints.restraint_duration_hours,
        stg_restraints_flowsheets_raw.recorded_date
),

q2_flowsheets as (
    --region subcomponents of 2-hour observation flowsheets
    --separate CTE used due to different cadence
    select
        stg_restraints.restraint_episode_key,
        stg_restraints_flowsheets_raw.recorded_date,
        lead(stg_restraints_flowsheets_raw.recorded_date) over(
            partition by
                stg_restraints.restraint_episode_key
            order by
                stg_restraints_flowsheets_raw.recorded_date
        ) as next_recorded_date,
        --hours_between would truncate the minutes between hours, but anything >2 hours is non-compliant
        minutes_between(next_recorded_date, stg_restraints_flowsheets_raw.recorded_date) / 60.0 as q2_time_diff,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071704 --Range of Motion
                then 1 else 0 end) as q2_rom_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071787 --Elimination (Hygiene Needs)
                then 1 else 0 end) as q2_hygiene_ind,
        max(case when stg_restraints_flowsheets_raw.flowsheet_id = 40071789 --Fluids/Food/Meal
                then 1 else 0 end) as q2_nutrition_ind,
        case
            when stg_restraints.restraint_duration_hours <= 2
                then -1
            else (q2_rom_ind + q2_hygiene_ind + q2_nutrition_ind) / 3 end as q2_rows_complete_ind
    from
        {{ ref('stg_restraints') }} as stg_restraints
        left join {{ ref('stg_restraints_flowsheets_raw') }} as stg_restraints_flowsheets_raw
            on stg_restraints.visit_key = stg_restraints_flowsheets_raw.visit_key
            and stg_restraints_flowsheets_raw.recorded_date between
            stg_restraints.restraint_start and stg_restraints.restraint_removal
            and stg_restraints_flowsheets_raw.flowsheet_id in (
                40071704, --Range of Motion
                40071787, --Elimination (Hygiene Needs)
                40071789 --Fluids/Food/Meal
            )
    group by
        stg_restraints.restraint_episode_key,
        stg_restraints.restraint_duration_hours,
        stg_restraints_flowsheets_raw.recorded_date
--end region
)

--region merge to encounter level
select
    stg_restraints.restraint_episode_key,
    /*Violent Restraint*/
    max(case when flowsheets_per_timestamp.flowsheet_number = 1
        then flowsheets_per_timestamp.fs_factors_ind end) as initial_fs_factors_ind,
    max(case when flowsheets_per_timestamp.flowsheet_number = 1
        then flowsheets_per_timestamp.fs_alternative_ind end) as initial_fs_alternative_ind,
    max(case when flowsheets_per_timestamp.flowsheet_number = 1
        then flowsheets_per_timestamp.fs_justification_ind end) as initial_fs_justification_ind,
    max(case when flowsheets_per_timestamp.flowsheet_number = 1
        then flowsheets_per_timestamp.fs_device_ind end) as initial_fs_device_ind,
    max(case when flowsheets_per_timestamp.flowsheet_number = 1
        then flowsheets_per_timestamp.fs_visual_observation_ind end) as initial_fs_visual_observation_ind,
    max(case when flowsheets_per_timestamp.flowsheet_number = 1
        then flowsheets_per_timestamp.fs_complete_ind end) as initial_fs_rows_complete_ind,
    /*Q15 stg_restraints_flowsheets_raw Completed at least every 15 minutes*/
    case
        --no way to tell how many checks there should be
        when stg_restraints.restraint_removal is null then 0
        --if more than 15 minutes passes between checks or a check is missed, non-compliant
        when max(flowsheets_per_timestamp.q15_time_diff) > 15
            --only one Q15 check is performed during the restraint
            or restraint_duration_hours > 0.25
                and min(flowsheets_per_timestamp.q15_time_diff) is null
            then 0 else 1 end as q15_every_15_min_ind,
    min(q15_rows_complete_ind) as all_q15_rows_complete_ind,
    /*Q2 flowsheets Completed at least every 2 hours*/
    case
        --no way to tell how many checks there should be
        when stg_restraints.restraint_removal is null then 0
        --if more than 2 hours passes between checks or a check is missed, non-compliant
        when max(q2_flowsheets.q2_time_diff) > 2
            --only one Q2 check is performed during the restraint
            or stg_restraints.restraint_duration_hours > 2
                and min(q2_flowsheets.q2_time_diff) is null
            then 0 else 1 end as q2_every_2_hrs_ind,
    min(q2_flowsheets.q2_rows_complete_ind) as all_q2_rows_complete_ind
from
    {{ ref('stg_restraints') }} as stg_restraints
    left join flowsheets_per_timestamp
        on stg_restraints.restraint_episode_key = flowsheets_per_timestamp.restraint_episode_key
    left join q2_flowsheets
        on stg_restraints.restraint_episode_key = q2_flowsheets.restraint_episode_key
group by
    stg_restraints.restraint_episode_key,
    stg_restraints.restraint_removal,
    stg_restraints.restraint_duration_hours
