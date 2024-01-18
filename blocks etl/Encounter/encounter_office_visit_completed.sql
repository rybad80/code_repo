with visit_events as (
    select
        encounter_office_visit_all.visit_key,
        max(case
                when lower(procedure_order_clinical.procedure_group_name) = 'imaging'
                    and lower(procedure_order_clinical.order_status) = 'completed' then 1
                else 0
        end) as imaging_ind,
        max(case
                when lower(procedure_order_clinical.procedure_name) like '%cast%'
                    and lower(procedure_order_clinical.order_status) = 'completed' then 1
                else 0
        end) as cast_ind,
        max(case
                when lower(procedure_order_clinical.procedure_group_name) = 'lab'
                    and procedure_order_clinical.specimen_taken_date is not null then 1
                else 0
        end) as lab_ind
    from
        {{ref('encounter_office_visit_all')}} as encounter_office_visit_all
        inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
            on procedure_order_clinical.visit_key = encounter_office_visit_all.visit_key
    group by
        encounter_office_visit_all.visit_key
),
note_author_type as (
    select
        note_edit_metadata_history.visit_key,
        max(case
            when lower(note_edit_metadata_history.version_author_employee_title) like 'scribe%'
            then 1 else 0
        end) as scribe_ind,
        max(case
            when lower(note_edit_metadata_history.version_author_title) in ('crnp', 'pa', 'pa-c')
            then 1 else 0
        end) as app_ind,
        max(case
            when lower(note_edit_metadata_history.version_author_title) in ('md', 'do', 'md,phd', 'md,dmd')
            then 1 else 0
        end) as physician_ind,
        case
            when scribe_ind + app_ind = 0 and physician_ind = 1 then 'Physician'
            when app_ind = 1 and physician_ind = 1 then 'Physician + APP'
            when scribe_ind = 1 and physician_ind = 1 then 'Physician + Scribe'
            when physician_ind = 0 and app_ind = 1 then 'APP'
        end as note_author_type
    from
        {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
        inner join {{ref('encounter_specialty_care')}} as encounter_specialty_care
            on encounter_specialty_care.visit_key = note_edit_metadata_history.visit_key
    where
        note_edit_metadata_history.note_type_id = 1 --'progress notes'
        and note_edit_metadata_history.note_entry_date
            > encounter_specialty_care.check_in_date --removes pre-charting
    group by
        note_edit_metadata_history.visit_key
),
surgery_resulted as (
    select distinct
        encounter_office_visit_all.visit_key
    from
        {{ref('encounter_office_visit_all')}} as encounter_office_visit_all
        inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
            on procedure_order_clinical.visit_key = encounter_office_visit_all.visit_key
    where
        lower(procedure_order_clinical.procedure_name) like '%surg%case%'
),
surgeon_active_date as (
    select
        surgery_department_or_case_procedure.surgeon_prov_key,
        max(surgery_department_or_case_procedure.surgery_date) as most_recent_surgery
    from
        {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
    where
        surgery_department_or_case_procedure.case_status = 'Completed'
    group by
        surgery_department_or_case_procedure.surgeon_prov_key
)
select
    encounter_office_visit_all.visit_key,
    encounter_office_visit_all.patient_name,
    encounter_office_visit_all.mrn,
    encounter_office_visit_all.csn,
    encounter_office_visit_all.encounter_date,
    encounter_office_visit_all.sex,
    stg_patient.race_ethnicity,
    encounter_office_visit_all.dob,
    encounter_office_visit_all.age_years,
    encounter_office_visit_all.age_days,
    encounter_office_visit_all.provider_name,
    encounter_office_visit_all.provider_id,
    encounter_office_visit_all.provider_title,
    coalesce(note_author_type.note_author_type, 'Other') as note_author_type,
    encounter_office_visit_all.app_is_primary_ind,
    encounter_office_visit_all.app_ind,
    encounter_office_visit_all.department_location,
    encounter_office_visit_all.department_name,
    encounter_office_visit_all.department_id,
    encounter_office_visit_all.specialty,
    encounter_office_visit_all.payor_group,
    encounter_office_visit_all.visit_type,
    encounter_office_visit_all.visit_type_id,
    encounter_office_visit_all.encounter_type,
    encounter_office_visit_all.encounter_type_id,
    encounter_office_visit_all.appointment_date,
    encounter_office_visit_all.scheduled_appointment_time_of_day,
    encounter_office_visit_all.appointment_made_date,
    encounter_office_visit_all.appointment_note_text,
    stg_encounter.los_proc_cd as level_service_procedure_code,
    encounter_specialty_care.scheduled_length_min,
    extract(
        epoch from
        encounter_specialty_care.start_rooming_date
        - (case --max timestamp between check-in time and scheduled start time
            when date(encounter_specialty_care.check_in_date) != date(encounter_specialty_care.appointment_date)
                then null
            when encounter_specialty_care.start_rooming_date < encounter_specialty_care.appointment_date
                then encounter_specialty_care.check_in_date
            when encounter_specialty_care.appointment_date > encounter_specialty_care.check_in_date
                then encounter_specialty_care.appointment_date
            else encounter_specialty_care.check_in_date
            end)
        ) / 60.0 as check_in_to_room_min,
    extract(
            epoch from
            (case
                when date(
                    coalesce(encounter_specialty_care.check_out_date, encounter_specialty_care.complete_visit_date)
                    ) = encounter_specialty_care.encounter_date
                then coalesce(
                    encounter_specialty_care.check_out_date,
                    encounter_specialty_care.complete_visit_date
                ) else null
            end
            ) - (case --max timestamp between check-in time and scheduled start time
                when date(encounter_specialty_care.check_in_date)
                    != date(encounter_specialty_care.appointment_date)
                    then null
                when encounter_specialty_care.start_rooming_date < encounter_specialty_care.appointment_date
                    then encounter_specialty_care.check_in_date
                when encounter_specialty_care.appointment_date > encounter_specialty_care.check_in_date
                    then encounter_specialty_care.appointment_date
                else encounter_specialty_care.check_in_date
                end
            )
        ) / 60.0 as throughput_min,
    stg_encounter_office_visit_diagnosis.primary_diagnosis_code,
    stg_encounter_office_visit_diagnosis.primary_diagnosis_name,
    stg_encounter_office_visit_diagnosis.visit_diagnosis_list,
    stg_encounter_office_visit_diagnosis.all_problem_list,
    stg_encounter_office_visit_diagnosis.all_diagnosis_list,
    coalesce(visit_events.imaging_ind, 0) as imaging_at_visit_ind,
    coalesce(visit_events.cast_ind, 0) as cast_at_visit_ind,
    coalesce(visit_events.lab_ind, 0) as labs_at_visit_ind,
    coalesce(note_author_type.scribe_ind, 0) as scribe_present_ind,
    encounter_office_visit_all.fiscal_year,
    encounter_office_visit_all.calendar_year,
    encounter_office_visit_all.calendar_month,
    encounter_office_visit_all.days_to_appointment,
    encounter_office_visit_all.referring_provider_name,
    encounter_office_visit_all.primary_care_location,
    stg_encounter_office_visit_diagnosis.complex_chronic_condition_ind,
    stg_encounter_office_visit_diagnosis.medically_complex_ind,
    encounter_specialty_care.physician_service_level_ind,
    encounter_office_visit_all.new_to_specialty_3_yr_ind,
    encounter_office_visit_all.mychop_ever_used_ind,
    encounter_office_visit_all.mychop_curently_active_ind,
    encounter_office_visit_all.mychop_active_on_encounter_ind,
    case
        when surgeon_active_date.surgeon_prov_key is null then null
        when surgery_resulted.visit_key is not null then 1
        else 0
    end as surgical_case_requested_ind,
    case when encounter_specialty_care.check_out_date is null then 1 else 0 end as missing_check_out_ind,
    case
        when encounter_specialty_care.check_in_date > encounter_specialty_care.appointment_date then 1
        else 0
    end as late_to_visit_ind,
    case when date(visit.enc_close_dt) = stg_encounter.encounter_date then 1 else 0 end as chart_closed_1_day_ind,
    case
        when date(visit.enc_close_dt) - stg_encounter.encounter_date <= 3
        then 1
        else 0
        end as chart_closed_3_day_ind,
    encounter_office_visit_all.video_visit_ind,
    case
        when lower(encounter_office_visit_all.payor_group) in ('medical assistance', 'government')
        then 1
        else 0
    end as govt_payor_ind,
    encounter_office_visit_all.patient_address_seq_num,
    encounter_office_visit_all.patient_address_zip_code,
    encounter_office_visit_all.dept_key,
    encounter_office_visit_all.prov_key,
    encounter_office_visit_all.pat_key
from
    {{ref('encounter_office_visit_all')}} as encounter_office_visit_all
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = encounter_office_visit_all.visit_key
    inner join {{ref('encounter_specialty_care')}} as encounter_specialty_care
        on encounter_specialty_care.visit_key = encounter_office_visit_all.visit_key
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = encounter_office_visit_all.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = encounter_office_visit_all.pat_key
    left join chop_analytics.admin.stg_encounter_office_visit_diagnosis
        on stg_encounter_office_visit_diagnosis.visit_key = encounter_office_visit_all.visit_key
    left join visit_events
        on visit_events.visit_key = encounter_office_visit_all.visit_key
    left join note_author_type
        on note_author_type.visit_key = encounter_office_visit_all.visit_key
    left join surgery_resulted
        on surgery_resulted.visit_key = encounter_office_visit_all.visit_key
    left join surgeon_active_date
        on encounter_office_visit_all.prov_key = surgeon_active_date.surgeon_prov_key
        and encounter_office_visit_all.encounter_date - surgeon_active_date.most_recent_surgery <= (365.25 / 2)
where
    encounter_office_visit_all.cancel_noshow_ind = 0
    and (
        encounter_office_visit_all.appointment_status_id != 1 -- scheduled
        or (
            encounter_office_visit_all.appointment_status_id != -2 -- not applicable
            and encounter_office_visit_all.encounter_type_id = '76' -- telehealth
        )
    )
