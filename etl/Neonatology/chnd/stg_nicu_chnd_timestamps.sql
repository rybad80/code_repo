with complete_nicu_visit as (
    --model data to capture entire NICU visit
    select
        neo_nicu_episode_phl.visit_key,
        min(neo_nicu_episode_phl.episode_start_date) as nicu_admit_date,
        --hack the data in order to be able to take a max AND still get null if patient is in NICU
        case
            when max(coalesce(neo_nicu_episode_phl.episode_end_date, current_date)) != current_date
            then max(neo_nicu_episode_phl.episode_end_date)
        end as nicu_discharge_date
    from
        {{ ref('neo_nicu_episode_phl') }} as neo_nicu_episode_phl
    group by
        neo_nicu_episode_phl.visit_key
)

select
    surgery_encounter.pat_key,
    surgery_encounter.visit_key,
    surgery_encounter.log_key,
    surgery_encounter.anes_visit_key,
    surgery_encounter.mrn,
    surgery_encounter.patient_name,
    surgery_encounter.surgery_date,
    surgery_encounter.location,
    surgery_encounter.service,
    surgery_encounter.primary_surgeon,
    surgery_encounter.room,
    surgery_encounter_timestamps.in_preop_room_date,
    surgery_encounter_timestamps.in_room_date,
    surgery_encounter_timestamps.anesthesia_start_date,
    surgery_encounter_timestamps.procedure_start_date - interval '3 days' as pre_procedure_start,
    surgery_encounter_timestamps.procedure_start_date,
    surgery_encounter_timestamps.procedure_close_date,
    surgery_encounter_timestamps.anesthesia_stop_date,
    surgery_encounter_timestamps.out_room_date,
    surgery_encounter_timestamps.recovery_exit_date,
    surgery_encounter.first_panel_first_procedure_name,
    case
        when surgery_encounter_timestamps.in_room_date < complete_nicu_visit.nicu_admit_date then 1 else 0
    end as admitted_after_surgery_ind,
    case
        when surgery_encounter_timestamps.in_room_date > complete_nicu_visit.nicu_discharge_date then 1 else 0
    end as discharged_after_surgery_ind,
    case
        when lower(surgery_encounter.room) not like '%cardiac%'
            and lower(surgery_encounter.room) not like '%fetal%'
            and lower(surgery_encounter.room) not like '%c section%'
            and lower(surgery_encounter.first_panel_first_procedure_name) not like '%ecmo%'
            and discharged_after_surgery_ind = 0
        then 1 else 0
    end as stepp_ind,
    case when lower(surgery_encounter.room) like '%nicu%' then 1 else 0 end as nicu_surgery_ind,
    max(
        case
            when flowsheet_all.recorded_date <= coalesce(
                surgery_encounter_timestamps.in_preop_room_date, surgery_encounter_timestamps.in_room_date
                )
            then flowsheet_all.recorded_date
        end
    ) as time_leave,
    min(
        case
            when flowsheet_all.recorded_date > coalesce(
                surgery_encounter_timestamps.out_room_date, surgery_encounter_timestamps.procedure_close_date
                )
            then flowsheet_all.recorded_date
        end
    ) as time_return,
    min(
        case
            when flowsheet_all.recorded_date > coalesce(
                recovery_exit_date - interval '5 minutes',
                complete_nicu_visit.nicu_admit_date - interval '5 minutes'
                )
            then flowsheet_all.recorded_date
        end
    ) as time_return_admit_after,
    case
        when nicu_surgery_ind = 1 then surgery_encounter_timestamps.anesthesia_start_date
        when admitted_after_surgery_ind = 1 then '1900-01-01'::date
        else time_leave
    end as leave_nicu_time,
    case
        when nicu_surgery_ind = 1 then surgery_encounter_timestamps.anesthesia_stop_date
        when admitted_after_surgery_ind = 1 then time_return_admit_after
        else time_return
    end as return_nicu_time,
    case
        when nicu_surgery_ind = 1 then surgery_encounter_timestamps.anesthesia_start_date - interval '1 hour'
        else time_leave - interval '1 hour'
    end as pre_temp_window,
    case
        when nicu_surgery_ind = 1 then surgery_encounter_timestamps.anesthesia_stop_date + interval '30 minutes'
        when admitted_after_surgery_ind = 0 then time_return + interval '30 minutes'
        else time_return_admit_after + interval '30 minutes'
    end as post_temp_window,
    case
        when nicu_surgery_ind = 1 then surgery_encounter_timestamps.anesthesia_start_date - interval '24 hours'
        else time_leave - interval '24 hours'
    end as pre_lab_window,
    return_nicu_time + interval '65 minutes' as post_lab_window,
    return_nicu_time + interval '24 hours' as post_pain_window
from
    {{ ref('surgery_encounter') }} as surgery_encounter
    inner join {{ ref('surgery_encounter_timestamps') }} as surgery_encounter_timestamps
        on surgery_encounter.log_key = surgery_encounter_timestamps.log_key
    inner join {{ ref('surgery_encounter_pre_post_department') }}  as surgery_encounter_pre_post_department
        on surgery_encounter.log_key = surgery_encounter_pre_post_department.log_key
    inner join {{ ref('neo_nicu_episode_phl') }} as neo_nicu_episode_phl
        on neo_nicu_episode_phl.visit_key = surgery_encounter_timestamps.visit_key
    inner join complete_nicu_visit
        on complete_nicu_visit.visit_key = surgery_encounter_timestamps.visit_key
    inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on surgery_encounter.visit_key = flowsheet_all.visit_key
    inner join {{ ref('flowsheet_group_lookup') }} as flowsheet_group_lookup
        on flowsheet_all.fs_key = flowsheet_group_lookup.fs_key
where
    flowsheet_group_lookup.template_id = 40001002 --Vital Signs NICU
    and (
        lower(surgery_encounter_pre_post_department.preop_department_group_abbr) = 'nicu'
        or lower(surgery_encounter_pre_post_department.postop_department_group_abbr) = 'nicu'
        or date(neo_nicu_episode_phl.episode_start_date) = date(surgery_encounter_timestamps.in_room_date)
        or date(neo_nicu_episode_phl.episode_end_date) = date(surgery_encounter_timestamps.procedure_close_date)
    )
group by
    surgery_encounter.pat_key,
    surgery_encounter.visit_key,
    surgery_encounter.log_key,
    surgery_encounter.anes_visit_key,
    surgery_encounter.mrn,
    surgery_encounter.patient_name,
    surgery_encounter.surgery_date,
    surgery_encounter.location,
    surgery_encounter.service,
    surgery_encounter.primary_surgeon,
    surgery_encounter.room,
    surgery_encounter_timestamps.in_preop_room_date,
    surgery_encounter_timestamps.in_room_date,
    surgery_encounter_timestamps.anesthesia_start_date,
    surgery_encounter_timestamps.procedure_start_date,
    surgery_encounter_timestamps.procedure_close_date,
    surgery_encounter_timestamps.anesthesia_stop_date,
    surgery_encounter_timestamps.out_room_date,
    surgery_encounter_timestamps.recovery_exit_date,
    surgery_encounter.first_panel_first_procedure_name,
    complete_nicu_visit.nicu_admit_date,
    complete_nicu_visit.nicu_discharge_date
