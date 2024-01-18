with goal_to_restraint_bounds as (
    --region manual and short restraints can still be compliant if plan of care updated after restraint removal
    --right bound goals ensuring restraint was not completed before initaition
    --left bound goals with a "run" to apply goals to restraints
    select
        stg_restraints_cohort.epsd_start_key,
        stg_restraints_cohort.device_type,
        stg_restraints_goals.goal_id,
        stg_restraints_goals.goal_create_dttm,
        case when stg_restraints_goals.goal_create_dttm < stg_restraints_cohort.restraint_removal
            then 1 else 0 end as goal_after_start_ind,
        max(goal_after_start_ind) over(
            partition by
                stg_restraints_cohort.visit_key,
                stg_restraints_goals.goal_id
            order by
                stg_restraints_cohort.restraint_start
            rows between current row and unbounded following
        ) as goal_run_ind
    from
        {{ ref('stg_restraints_cohort') }} as stg_restraints_cohort
        inner join {{ ref('stg_restraints_goals') }} as stg_restraints_goals
            on stg_restraints_cohort.pat_id = stg_restraints_goals.pat_id
    where
        --the goal was not completed before restraint initiation
        --last edit date is used as a proxy for the goal being met
        /*we don't know the goal's exact completion time*/
         stg_restraints_goals.clty_last_edit_dt >= date(stg_restraints_cohort.restraint_start)
    group by
        stg_restraints_cohort.epsd_start_key,
        stg_restraints_cohort.device_type,
        stg_restraints_cohort.visit_key,
        stg_restraints_cohort.restraint_start,
        stg_restraints_cohort.restraint_removal,
        stg_restraints_goals.goal_id,
        stg_restraints_goals.goal_create_dttm
)

select
    epsd_start_key,
    max(goal_run_ind) as care_plan_updated_ind
from
    goal_to_restraint_bounds
group by
    epsd_start_key
