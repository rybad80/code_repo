with autism_diagnosis as (
    --region encounters where a patient with autism was seen
    select
        visit_key,
        1 as autism_dx
    from
        {{ ref('diagnosis_encounter_all') }}
    where
        lower(icd10_code) = 'f84.0'
    group by
        visit_key
),

violent_restraint_orders as (
    --region determine compliance of the first ten orders (based on redcap)
    select
        stg_restraints_cohort.restraint_episode_key,
        max(stg_restraints_orders.order_number) as num_orders_placed,
        min(stg_restraints_orders.placed_date) as initial_order_time,
        minutes_between(
            stg_restraints_cohort.restraint_start, initial_order_time) as initial_order_to_restraint_minutes,
        --order placed within 15 minutes of initiation
        case when abs(initial_order_to_restraint_minutes) <= 15
            then 1 else 0 end as initial_order_timing_compliant_ind,
        /*Subsequent Orders*/
        --Must be completed every 1 or 2 hrs, depending on patient's age, to be compliant
        max(case when stg_restraints_orders.order_number = 2
            then stg_restraints_orders.placed_date end) as second_order_time,
        minutes_between(
            second_order_time, stg_restraints_cohort.restraint_start) / 60.0 as second_order_to_restraint_hrs,
        case
            when second_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration
                then 1
            when second_order_time is null
                then null
            else 0 end as second_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 3
            then stg_restraints_orders.placed_date end) as third_order_time,
        minutes_between(
            third_order_time, stg_restraints_cohort.restraint_start) / 60.0 as third_order_to_restraint_hrs,
        case
            when third_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 2
                then 1
            when third_order_time is null
                then null
            else 0 end as third_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 4
            then stg_restraints_orders.placed_date end) as fourth_order_time,
        minutes_between(
            fourth_order_time, stg_restraints_cohort.restraint_start) / 60.0 as fourth_order_to_restraint_hrs,
        case
            when fourth_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 3
                then 1
            when fourth_order_time is null
                then null
            else 0 end as fourth_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 5
            then stg_restraints_orders.placed_date end) as fifth_order_time,
        minutes_between(
            fifth_order_time, stg_restraints_cohort.restraint_start) / 60.0 as fifth_order_to_restraint_hrs,
        case
            when fifth_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 4
                then 1
            when fifth_order_time is null
                then null
            else 0 end as fifth_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 6
            then stg_restraints_orders.placed_date end) as sixth_order_time,
        minutes_between(
            sixth_order_time, stg_restraints_cohort.restraint_start) / 60.0 as sixth_order_to_restraint_hrs,
        case
            when sixth_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 5
                then 1
            when sixth_order_time is null
                then null
            else 0 end as sixth_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 7
            then stg_restraints_orders.placed_date end) as seventh_order_time,
        minutes_between(
            seventh_order_time, stg_restraints_cohort.restraint_start) / 60.0 as seventh_order_to_restraint_hrs,
        case
            when seventh_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 6
                then 1
            when seventh_order_time is null
                then null
            else 0 end as seventh_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 8
            then stg_restraints_orders.placed_date end) as eighth_order_time,
        minutes_between(
            eighth_order_time, stg_restraints_cohort.restraint_start) / 60.0 as eighth_order_to_restraint_hrs,
        case
            when eighth_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 7
                then 1
            when eighth_order_time is null
                then null
            else 0 end as eighth_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 9
            then stg_restraints_orders.placed_date end) as ninth_order_time,
        minutes_between(
            ninth_order_time, stg_restraints_cohort.restraint_start) / 60.0 as ninth_order_to_restraint_hrs,
        case
            when ninth_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 8
                then 1
            when ninth_order_time is null
                then null
            else 0 end as ninth_order_compliant_ind,
        max(case when stg_restraints_orders.order_number = 10
            then stg_restraints_orders.placed_date end) as tenth_order_time,
        minutes_between(
            tenth_order_time, stg_restraints_cohort.restraint_start) / 60.0 as tenth_order_to_restraint_hrs,
        case
            when tenth_order_to_restraint_hrs < stg_restraints_cohort.age_order_duration * 9
                then 1
            when tenth_order_time is null
                then null
                else 0 end as tenth_order_compliant_ind,
        case when num_orders_placed < 2
            then 0 else num_orders_placed - 1 end  as num_subsequent_orders,
        (coalesce(second_order_compliant_ind, 0)
            + coalesce(third_order_compliant_ind, 0)
            + coalesce(fourth_order_compliant_ind, 0)
            + coalesce(fifth_order_compliant_ind, 0)
            + coalesce(sixth_order_compliant_ind, 0)
            + coalesce(seventh_order_compliant_ind, 0)
            + coalesce(eighth_order_compliant_ind, 0)
            + coalesce(ninth_order_compliant_ind, 0)
            + coalesce(tenth_order_compliant_ind, 0)
        ) as subsequent_orders_compliant,
        (coalesce(second_order_compliant_ind, 1)
            + coalesce(third_order_compliant_ind, 1)
            + coalesce(fourth_order_compliant_ind, 1)
            + coalesce(fifth_order_compliant_ind, 1)
            + coalesce(sixth_order_compliant_ind, 1)
            + coalesce(seventh_order_compliant_ind, 1)
            + coalesce(eighth_order_compliant_ind, 1)
            + coalesce(ninth_order_compliant_ind, 1)
            + coalesce(tenth_order_compliant_ind, 1)
        ) / 9 as subsequent_orders_compliant_ind
    from
        {{ ref('stg_restraints_cohort') }} as stg_restraints_cohort
        left join {{ ref('stg_restraints_orders') }} as stg_restraints_orders
            on stg_restraints_cohort.restraint_episode_key = stg_restraints_orders.restraint_episode_key
    where stg_restraints_cohort.violent_restraint_ind = 1
    group by
        stg_restraints_cohort.restraint_episode_key,
        stg_restraints_cohort.restraint_start,
        stg_restraints_cohort.age_order_duration
)

select
    stg_restraints_cohort.restraint_episode_key,
    stg_restraints_cohort.device_type,
    stg_restraints_cohort.restraint_start,
    stg_restraints_cohort.restraint_removal,
    stg_restraints_cohort.restraint_duration_hours,
    stg_restraints_cohort.trial_release_ind,
    coalesce(autism_diagnosis.autism_dx, 0) as autism_dx,
    stg_restraints_cohort.num_orders_required,
    coalesce(violent_restraint_orders.num_orders_placed, 0) as num_orders_placed,
    case when violent_restraint_orders.num_orders_placed >= stg_restraints_cohort.num_orders_required
        then 1 else 0 end as num_orders_placed_compliant_ind,
    /*Restraint Orders*/
    violent_restraint_orders.initial_order_time,
    violent_restraint_orders.initial_order_to_restraint_minutes,
    violent_restraint_orders.initial_order_timing_compliant_ind,
    coalesce(initial_order_questions.order_alternatives_ind, 0) as initial_order_alternatives_ind,
    coalesce(initial_order_questions.order_rationale_ind, 0) as initial_order_rationale_ind,
    coalesce(initial_order_questions.order_restraint_method_ind, 0) as initial_order_restraint_method_ind,
    coalesce(initial_order_questions.device_ind, 0) as initial_order_device_ind,
    coalesce(initial_order_questions.order_complete_ind, 0) as initial_order_complete_ind,
    coalesce(violent_restraint_orders.num_subsequent_orders, 0) as num_subsequent_orders,
    coalesce(violent_restraint_orders.subsequent_orders_compliant, 0) as subsequent_orders_compliant,
    violent_restraint_orders.subsequent_orders_compliant_ind,
    /*Face-to-Face Evaluation Note*/
    case when stg_restraints_floc_notes_initial.first_eval_time is not null
        then 1 else 0 end as face_to_face_performed_ind,
    stg_restraints_floc_notes_initial.first_eval_time,
    coalesce(stg_restraints_floc_notes_initial.face_to_face_within_hr_ind, 0) as face_to_face_within_hr_ind,
    coalesce(stg_restraints_floc_notes_initial.floc_reasons_ind, 0) as floc_reasons_ind,
    coalesce(stg_restraints_floc_notes_initial.floc_pat_response_ind, 0) as floc_pat_response_ind,
    coalesce(stg_restraints_floc_notes_initial.floc_attending_notified_ind, 0) as floc_attending_notified_ind,
    coalesce(stg_restraints_floc_notes_initial.floc_summary_ind, 0) as floc_summary_ind,
    coalesce(stg_restraints_floc_notes_initial.note_complete_ind, 0) as floc_note_complete_ind,
    /*Initial Violent Restraint Flowsheet*/
    stg_restraints_flowsheets.initial_fs_factors_ind,
    stg_restraints_flowsheets.initial_fs_alternative_ind,
    stg_restraints_flowsheets.initial_fs_justification_ind,
    stg_restraints_flowsheets.initial_fs_device_ind,
    stg_restraints_flowsheets.initial_fs_visual_observation_ind,
    stg_restraints_flowsheets.initial_fs_rows_complete_ind,
    /*Q15 Flowsheets Completed at least every 15 minutes*/
    stg_restraints_flowsheets.q15_every_15_min_ind,
    stg_restraints_flowsheets.all_q15_rows_complete_ind,
    /*Q2 Flowsheets Completed at least every 2 hours*/
    stg_restraints_flowsheets.q2_every_2_hrs_ind,
    stg_restraints_flowsheets.all_q2_rows_complete_ind,
    /*Plan of Care*/
    --Plan of Care is not completed in ED or EDECU; ignore this component
    --otherwise, a goal needs to be active during restraint
    case when stg_restraints_cohort.department_name in (
            'MAIN EMERGENCY DEPT',
            'ED EXTENDED CARE UN*',
            'KOPH EMERGENCY DEP'
        ) then -1
        else coalesce(stg_restraints_care_plan.care_plan_updated_ind, 0) end as plan_of_care_updated_ind,
    /*Overall Compliance*/
    (violent_restraint_orders.initial_order_timing_compliant_ind
        + coalesce(initial_order_questions.order_complete_ind, 0)
        + num_orders_placed_compliant_ind
        + violent_restraint_orders.subsequent_orders_compliant_ind
        + coalesce(stg_restraints_floc_notes_initial.face_to_face_within_hr_ind, 0)
        + coalesce(stg_restraints_floc_notes_initial.note_complete_ind, 0)
        + stg_restraints_flowsheets.initial_fs_rows_complete_ind
        + stg_restraints_flowsheets.q15_every_15_min_ind
        + abs(stg_restraints_flowsheets.all_q15_rows_complete_ind)
        + stg_restraints_flowsheets.q2_every_2_hrs_ind
        + abs(stg_restraints_flowsheets.all_q2_rows_complete_ind)
        + abs(plan_of_care_updated_ind)
    ) as numerator,
    12.0 as denominator,
    floor(numerator / denominator) as overall_compliance_ind,
    numerator / denominator as overall_compliance_score
from
    {{ ref('stg_restraints_cohort') }} as stg_restraints_cohort
    left join autism_diagnosis
        on stg_restraints_cohort.visit_key = autism_diagnosis.visit_key
    left join violent_restraint_orders
        on stg_restraints_cohort.restraint_episode_key = violent_restraint_orders.restraint_episode_key
    left join {{ ref('stg_restraints_order_questions')}} as initial_order_questions
        on stg_restraints_cohort.restraint_episode_key = initial_order_questions.restraint_episode_key
    left join {{ ref('stg_restraints_floc_notes_initial') }} as stg_restraints_floc_notes_initial
        on stg_restraints_cohort.restraint_episode_key = stg_restraints_floc_notes_initial.restraint_episode_key
    left join {{ ref('stg_restraints_flowsheets') }} as stg_restraints_flowsheets
        on stg_restraints_cohort.restraint_episode_key = stg_restraints_flowsheets.restraint_episode_key
    left join {{ ref('stg_restraints_care_plan') }} as stg_restraints_care_plan
        on stg_restraints_cohort.epsd_start_key = stg_restraints_care_plan.epsd_start_key
where stg_restraints_cohort.violent_restraint_ind = 1
