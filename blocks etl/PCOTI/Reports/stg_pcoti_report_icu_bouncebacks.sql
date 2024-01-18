with bouncebacks as (
    select
        pcoti_icu_transfers_left.episode_key,
        pcoti_icu_transfers_left.pat_key,
        pcoti_icu_transfers_left.visit_key,
        pcoti_icu_transfers_left.episode_event_key as index_event_key,
        pcoti_icu_transfers_left.to_department_group_name as index_to_department_group_name,
        pcoti_icu_transfers_left.to_campus_name as index_to_campus_name,
        pcoti_icu_transfers_left.icu_enter_date as index_icu_enter_date,
        pcoti_icu_transfers_left.icu_exit_date as index_icu_exit_date,
        pcoti_icu_transfers_left.from_ip_service_name as index_from_ip_service_name,
        pcoti_icu_transfers_left.from_department_name as index_from_department_name,
        pcoti_icu_transfers_left.from_department_group_name as index_from_department_group_name,
        pcoti_icu_transfers_left.from_campus_name as index_from_campus_name,
        pcoti_icu_transfers_left.next_department_name as index_next_department_name,
        pcoti_icu_transfers_left.next_department_group_name as index_next_department_group_name,
        pcoti_icu_transfers_left.next_campus_name as index_next_campus_name,
        pcoti_icu_transfers_right.episode_event_key as bb_episode_event_key,
        pcoti_icu_transfers_right.icu_enter_date as bb_icu_enter_date,
        pcoti_icu_transfers_right.icu_exit_date as bb_icu_exit_date,
        pcoti_icu_transfers_right.from_department_name as bb_from_department_name,
        pcoti_icu_transfers_right.from_department_group_name as bb_from_department_group_name,
        pcoti_icu_transfers_right.from_campus_name as bb_from_campus_name,
        case
            when pcoti_icu_transfers_right.intubation_pre1hr_post1hr_ind = 1 then 'Yes'
            else 'No'
        end as bb_intubation_pre1hr_post1hr_ind,
        case
            when pcoti_icu_transfers_right.vasopressor_pre1hr_post1hr_ind = 1 then 'Yes'
            else 'No'
        end as bb_vasopressor_pre1hr_post1hr_ind,
        case
            when pcoti_icu_transfers_right.fluid_bolus_pre1hr_post1hr_ind = 1 then 'Yes'
            else 'No'
        end as bb_fluid_bolus_pre1hr_post1hr_ind,
        case
            when pcoti_icu_transfers_right.cat_code_pre24hr_ind = 1 then 'Yes'
            else 'No'
        end as bb_cat_code_pre24hr_ind,
        pcoti_icu_transfers_right.intubation_first_start_date as bb_intubation_first_start_date,
        pcoti_icu_transfers_right.vasopressor_first_start_date as bb_vasopressor_first_start_date,
        pcoti_icu_transfers_right.fluid_bolus_gt60_date as bb_fluid_bolus_gt60_date,
        pcoti_icu_transfers_right.cat_code_first_date as bb_cat_code_first_date,
        case
            when pcoti_icu_transfers_right.unplanned_transfer_ind = 1 then 'Yes'
            else 'No'
        end as bb_unplanned_transfer_ind,
        case
            when pcoti_icu_transfers_right.emergent_transfer_ind = 1 then 'Yes'
            else 'No'
        end as bb_emergent_transfer_ind,
        row_number() over (
            partition by pcoti_icu_transfers_left.episode_event_key
            order by pcoti_icu_transfers_right.icu_enter_date
        ) as icu_return_xfer_seq
    from
        {{ ref('pcoti_icu_transfers') }} as pcoti_icu_transfers_left
        left join {{ ref('pcoti_icu_transfers') }} as pcoti_icu_transfers_right
            on pcoti_icu_transfers_left.episode_key = pcoti_icu_transfers_right.episode_key
            and pcoti_icu_transfers_right.icu_enter_date > pcoti_icu_transfers_left.icu_exit_date
            and pcoti_icu_transfers_right.icu_enter_date <= (
                pcoti_icu_transfers_left.icu_exit_date + interval '6 hours'
            )
    where
        pcoti_icu_transfers_left.next_event_type_abbrev = 'LOC_FLOOR'
        and pcoti_icu_transfers_right.prev_event_type_abbrev = 'LOC_FLOOR'
        and pcoti_icu_transfers_right.gte_2hrs_in_icu_ind = 1
        and pcoti_icu_transfers_right.surg_proc_ind = 0
)

select
    bouncebacks.*,
    stg_patient.mrn,
    encounter_inpatient.csn,
    stg_patient.patient_name,
    stg_patient.dob
from
    bouncebacks
    inner join {{ ref('stg_patient') }} as stg_patient
        on bouncebacks.pat_key = stg_patient.pat_key
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on bouncebacks.visit_key = encounter_inpatient.visit_key
where
    icu_return_xfer_seq = 1
