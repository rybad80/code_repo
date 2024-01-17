/*Data Cleaning*/
with restraint_stop_start as (
    --region find the closest discontinued time per each limb start
    --if another limb restraint continues while this restraint is removed, this is a trial restraint
    select
        starting.visit_key,
        starting.group_name,
        starting.group_id,
        starting.limb_grouper,
        starting.recorded_date as start_time,
        min(ending.recorded_date) as end_time,
        max(case when continuing.recorded_date is not null
            then 1 else 0 end) as continuing_ind
    from
        {{ ref('stg_restraints_raw') }} as starting
        left join {{ ref('stg_restraints_raw') }} as ending
            on starting.visit_key = ending.visit_key
            and starting.group_id = ending.group_id
            --join on limb type to prevent continuity errors
            and starting.limb_grouper = ending.limb_grouper
            and ending.recorded_date > starting.recorded_date
            and ending.meas_val = 'Discontinued'
        --trial restraint: is another limb restraint continuing?
        left join {{ ref('stg_restraints_raw') }} as continuing
            on starting.visit_key = continuing.visit_key
            and starting.group_id = continuing.group_id
            and ending.recorded_date = continuing.recorded_date
            and continuing.meas_val = 'Continued'
    where
        starting.meas_val = 'Start'
    group by
        starting.visit_key,
        starting.group_name,
        starting.group_id,
        starting.limb_grouper,
        starting.recorded_date
--end region
),

simplify_start as (
    --region separate device restraints and manual holds; they should not interfere with each other
    --find earliest start per removal time and device type
    select
        visit_key,
        group_name,
        group_id,
        --separate manual restraints from device restraints
        case when limb_grouper = 'Manual Hold'
            then 'Manual' else 'Device' end as device_type,
        min(start_time) as restraint_start_date,
        continuing_ind,
        end_time
    from
        restraint_stop_start
    group by
        visit_key,
        group_name,
        group_id,
        device_type,
        continuing_ind,
        end_time
    --end region
),

simplify_end as (
    --region find latest removal per start, group, and device type
    select
        visit_key,
        group_name,
        group_id,
        device_type,
        restraint_start_date,
        max(end_time) as restraint_removal_date,
        max(continuing_ind) as continuing_ind
    from
        simplify_start
    group by
        visit_key,
        group_name,
        group_id,
        device_type,
        restraint_start_date
--end region
),

/*Create Restraint Runs*/
encased_dates as (
    --region identify restraint embedded within another of same group and device type
    select
        visit_key,
        group_name,
        group_id,
        device_type,
        simplify_end.restraint_start_date,
        simplify_end.restraint_removal_date,
        simplify_end.continuing_ind,
        max(case --exclude runs that are entirely within another
            when simplify_end.restraint_start_date > self_join.restraint_start_date
                and simplify_end.restraint_removal_date < self_join.restraint_removal_date
                then 1 else 0 end) as encased_ind
    from
        simplify_end
        inner join simplify_end as self_join
            using (visit_key, group_name, group_id, device_type)
    group by
        visit_key,
        group_name,
        group_id,
        device_type,
        simplify_end.restraint_start_date,
        simplify_end.restraint_removal_date,
        simplify_end.continuing_ind
--end region
),

next_dates as (
    --region identify if the next restraint of the same group and device type overlaps the current run
    select
        visit_key,
        group_name,
        group_id,
        device_type,
        restraint_start_date,
        restraint_removal_date,
        continuing_ind,
        --can this be re-written with lead?
        lag(restraint_start_date) over (
            partition by
                visit_key,
                device_type,
                group_id
            order by
                restraint_start_date desc,
                restraint_removal_date desc
        ) as next_start,
        case -- if next run is not between the start & end then this end date is a "real" end
            when next_start between restraint_start_date and restraint_removal_date
                then 0 else 1 end as end_of_run
    from
        encased_dates
    where
        encased_ind = 0
--end region
),

start_of_run as (
    --region identify the true beginning of a restraint
    select
        *,
        coalesce(
            /*if prior value is null then first restraint of encounter,
            if not null and the prior run ended, this is a new restraint*/
            lag(end_of_run) over (
                partition by
                    visit_key,
                    group_id
                order by
                    restraint_start_date,
                    restraint_removal_date,
                    end_of_run --useful if multiple restraints with same removal date
            ), 1) as start_of_run
    from
        next_dates
--end region
),

cume_sum as (
    --region enumerate the runs
    select
        *,
        sum(start_of_run) over(
            partition by
                visit_key,
                group_id
            order by
                restraint_start_date,
                restraint_removal_date,
                device_type
            rows unbounded preceding) as restraint_number
    from
        start_of_run
--end region
)

--region identify the extremes of each run, defining the entire restraint duration
select
    visit_key,
    group_name,
    group_id,
    device_type,
    case when group_id in (
            40071712, --(Retired) Non-Violent Restraints
            40071703 --Non-Violent Restraints
            ) then 1 else 0 end as non_violent_restraint_ind,
    case when group_id in (
            40071755 --Violent Restraints
            ) then 1 else 0 end as violent_restraint_ind,
    max(continuing_ind) as trial_release_ind,
    restraint_number,
    min(restraint_start_date) as restraint_start, --first start in a run
    max(restraint_removal_date) as restraint_removal, --last removal in a run
    minutes_between(restraint_removal,
        restraint_start) / 60.0 as restraint_duration_hours,
    --create keys for joining
    --epsd_start_key defines restraint initiation
    --not a primary key; there are intentional  duplicates
    group_id || '-' || visit_key || '-' || min(restraint_start_date) as epsd_start_key,
    --primary key defines unique restraint identifier
    {{
        dbt_utils.surrogate_key([
            'group_id',
            'visit_key',
            'restraint_start',
            'device_type'
        ])
    }} as restraint_episode_key
from
    cume_sum
group by
    visit_key,
    group_name,
    group_id,
    device_type,
    restraint_number,  --things in the same run will be grouped together
    violent_restraint_ind,
    non_violent_restraint_ind
having
    --based on Regulatory dashboard. waiting for feedback to change 
    restraint_start >= '2020-07-01'
