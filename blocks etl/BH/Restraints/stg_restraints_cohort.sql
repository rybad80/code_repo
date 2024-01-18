with ip_adt as (
    select
        stg_restraints.restraint_episode_key,
        adt_department.department_name
    from
        {{ ref('stg_restraints') }} as stg_restraints
        inner join {{ ref('adt_department') }} as adt_department
            on stg_restraints.visit_key = adt_department.visit_key
    where
        --use less than in case restraint initiated at transfer time
        (
            stg_restraints.restraint_start >= adt_department.enter_date
            and stg_restraints.restraint_start < adt_department.exit_date_or_current_date
        )
        --before admission; assign to first department
        or (
            stg_restraints.restraint_start < adt_department.enter_date
            and adt_department.all_department_order = 1
        )
        --after discharge; assign to last department
        or (
            stg_restraints.restraint_start > adt_department.enter_date
            and adt_department.last_department_ind = 1
        )
)

select
    stg_restraints.restraint_episode_key,
    stg_restraints.epsd_start_key,
    stg_restraints.visit_key,
    stg_encounter.pat_key,
    stg_encounter.pat_id,
    stg_restraints.group_id,
    stg_restraints.group_name,
    stg_restraints.violent_restraint_ind,
    stg_restraints.non_violent_restraint_ind,
    stg_restraints.device_type,
    stg_restraints.restraint_start,
    stg_restraints.restraint_removal,
    stg_restraints.trial_release_ind,
    stg_restraints.restraint_duration_hours,
    --if admitted and restraint occurred within an ADT service, use that department
    --stg_restraints initiated at time of department transfer are assigned to the later department
    --if admitted and restraint occurred before admission, use the first ADT department
    case when encounter_ed.visit_key is not null then 'EDECU'
        else coalesce(ip_adt.department_name, stg_encounter.department_name) end as department_name,
    case when hours_between(
        bh_dispositions_medical_clearance.order_mc_yes_date_time_first,
        stg_encounter.hospital_admit_date) < 72
        then 1 else 0 end as medical_clearance_72_hrs_ind,
    --age at restraint initiation
    months_between(stg_restraints.restraint_start, stg_encounter.dob) / 12 as age_at_restraint,
    /*the following two are valid for violent stg_restraints*/
    case -->9 yrs = 2 hr order, <9 yrs = 1 hr
        when age_at_restraint >= 9
            then 2 else 1 end as age_order_duration,
    ceil(
        stg_restraints.restraint_duration_hours / age_order_duration
    ) as num_orders_required
from
    {{ ref('stg_restraints') }} as stg_restraints
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_restraints.visit_key = stg_encounter.visit_key
    left join ip_adt
        on stg_restraints.restraint_episode_key = ip_adt.restraint_episode_key
    --if ED, determine if patient was in observation
    left join {{ ref('encounter_ed') }} as encounter_ed
        on stg_restraints.visit_key = encounter_ed.visit_key
        and stg_restraints.restraint_start between
            encounter_ed.edecu_admit_date and encounter_ed.edecu_discharge_date
    --was patient medically cleared within 72 hours of admission?
    left join {{ ref('bh_dispositions_medical_clearance') }} as bh_dispositions_medical_clearance
        on stg_restraints.visit_key = bh_dispositions_medical_clearance.visit_key
        --table currently has duplicates; use this to eliminate
        and bh_dispositions_medical_clearance.order_mc_yes_date_time_first is not null
where
    --case 1: restraint was correctly discontinued
    stg_restraints.restraint_removal is not null
    --case 2: restraint was NOT correctly discontinued AND the patient was discharged
    or stg_encounter.hospital_discharge_date is not null
