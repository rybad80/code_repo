with procedure_dialysis_notes as (
    select distinct
        note_edit_metadata_history.pat_key,
        note_edit_metadata_history.mrn,
        note_edit_metadata_history.visit_key,
        note_edit_metadata_history.department_name,
        date_trunc('day', service_date) as date_of_interest,
        'procedure note' as reason_pulled_in,
        smart_text_id
    from {{ ref('note_edit_metadata_history')}} as note_edit_metadata_history
        left join {{ source('cdw', 'note_smart_text_id')}} as note_smart_text_id
            on note_smart_text_id.note_key = note_edit_metadata_history.note_key
        left join {{ source('cdw', 'smart_text')}} as smart_text
            on smart_text.smart_text_key = note_smart_text_id.smart_text_key
    where  lower(note_type) = 'procedures'
        and lower(version_author_service_name) = 'nephrology'
        and service_date >= '2016-01-01'
        and (smart_text_id != 15050 --ip neph percutaneous renal biopsy procedure note
            or smart_text_id is null)
        and last_edit_ind = 1
),

days_on_dialysis_setup as (
    select distinct
        usnews_billing.pat_key,
        usnews_billing.mrn,
        usnews_billing.visit_key,
        usnews_billing.department_name as billing_department_name,
        null as note_department_name,
        null as hospital_admit_date,
        null as hospital_discharge_date,
        service_date as date_of_interest,
        'procedure note charge' as reason_pulled_in
    from {{ ref('usnews_billing') }} as usnews_billing
    inner join {{ source('cdw', 'fact_transaction') }} as fact_transaction
        on usnews_billing.tx_id = fact_transaction.tx_id
    inner join {{ source('cdw', 'place_of_service') }} as place_of_service
        on fact_transaction.pos_key = place_of_service.pos_key
    where
       lower(department_name) not in ('main ct scan', 'main ultrasound', 'bgr diag radiology')
       and place_of_service.pos_id = '262' --chop_inpatient
       and usnews_billing.question_number = 'g8'
union all
    select
        procedure_dialysis_notes.pat_key,
        procedure_dialysis_notes.mrn,
        procedure_dialysis_notes.visit_key,
        null as billing_department_name,
        procedure_dialysis_notes.department_name as note_department_name,
        hospital_admit_date,
        hospital_discharge_date,
        procedure_dialysis_notes.date_of_interest,
        procedure_dialysis_notes.reason_pulled_in
    from procedure_dialysis_notes
        inner join {{ ref('stg_encounter')}} as stg_encounter
            on procedure_dialysis_notes.visit_key = stg_encounter.visit_key
    where
        encounter_type_id = 3 --hospital encounter
        and appointment_status_id in (-2, 6, 2) -- n/a, arrived, completed
),
esrd_exclusion as (
    select
        days_on_dialysis_setup.pat_key,
        --taking earliest ESRD date, only a transplant will reset clock
        min(diagnosis_encounter_all.problem_noted_date) as problem_noted_date
    from days_on_dialysis_setup
    inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on days_on_dialysis_setup.pat_key = diagnosis_encounter_all.pat_key
    where
        icd10_code in ('Z99.2', 'N18.5', 'N18.6')
        and problem_list_ind = 1
    group by
        days_on_dialysis_setup.pat_key
),
stage as (
    select distinct
        submission_year,
        metric_id,
        days_on_dialysis_setup.pat_key,
        days_on_dialysis_setup.mrn,
        max(days_on_dialysis_setup.visit_key)
            over (partition by days_on_dialysis_setup.mrn, date_of_interest) as visit_key,
        max(note_department_name)
            over (partition by days_on_dialysis_setup.mrn, date_of_interest) as note_department_name,
        max(billing_department_name)
            over (partition by days_on_dialysis_setup.mrn, date_of_interest) as billing_department_name,
        max(hospital_admit_date)
            over (partition by days_on_dialysis_setup.mrn, date_of_interest) as  hospital_admit_date,
        max(hospital_discharge_date)
            over (partition by days_on_dialysis_setup.mrn, date_of_interest) as  hospital_discharge_date,
        date_of_interest,
        max(case when reason_pulled_in = 'procedure note' then 1 else 0 end)
            over (partition by days_on_dialysis_setup.mrn, date_of_interest) as procedure_note_for_pt_day_ind,
        max(case when reason_pulled_in = 'procedure note charge' then 1 else 0 end)
            over (partition by days_on_dialysis_setup.mrn, date_of_interest)
            as procedure_note_charge_for_pt_day_ind,
        case when max(coalesce(maintenance_dialysis_ind, 0))
            over (partition by days_on_dialysis_setup.mrn, date_of_interest) = 1 then 0 else 1 end
            as not_in_maintenance_dialysis_cohort_ind,
        case when date_of_interest > most_recent_transplant_date
                and date_of_interest < maintenance_dialysis_start_date then 1
            when date_of_interest < maintenance_dialysis_start_date and most_recent_transplant_date is null then 1
            when date_of_interest < min(maintenance_dialysis_start_date)
            over (partition by days_on_dialysis_setup.mrn) then 1
            else 0 end as dialysis_before_maintenance_start_ind,
        case when (((procedure_note_for_pt_day_ind = 1
            or procedure_note_charge_for_pt_day_ind = 1)
            and not_in_maintenance_dialysis_cohort_ind = 1)
            -- note or charge, and not in the maintenance dialysis cohort
            or ((procedure_note_for_pt_day_ind = 1
            or procedure_note_charge_for_pt_day_ind = 1)
            and dialysis_before_maintenance_start_ind = 1))
            -- note or charge, and before maintenance dialysis start if in maintenance dialysis cohort
            then 1 else 0 end as not_maintenance_dialysis_patient_day_ind,
        -- only flag patients that would count for acute dialysis
        case when esrd_exclusion.pat_key is not null and not_maintenance_dialysis_patient_day_ind = 1 then 1
            else 0 end as esrd_problem_list_exclusion_ind,
        case when not_maintenance_dialysis_patient_day_ind = 1
/*          Per Ben 1/3/2022 - We will look at ESRD at the patient level for patients that may count
            after review */
            and esrd_problem_list_exclusion_ind = 0
        then 1 else 0 end as acute_dialysis_patient_day_ind
    from days_on_dialysis_setup
        inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
            on question_number = 'g8'
        left join {{ref('nephrology_encounter_dialysis')}} as nephrology_encounter_dialysis
            on nephrology_encounter_dialysis.mrn = days_on_dialysis_setup.mrn
        left join esrd_exclusion
            on days_on_dialysis_setup.pat_key = esrd_exclusion.pat_key
    where date_of_interest >= usnews_metadata_calendar.start_date
        and date_of_interest <= usnews_metadata_calendar.end_date
)
select
    stage.submission_year,
    stage.metric_id,
    stg_patient.patient_name,
    stage.mrn,
    stg_patient.dob,
    stage.visit_key,
    stage.note_department_name,
    stage.billing_department_name,
    stage.hospital_admit_date,
    stage.hospital_discharge_date,
    stage.date_of_interest,
    (date(date_of_interest) - date(stg_patient.dob)) / 365.25 as age_years,
    stage.procedure_note_for_pt_day_ind,
    stage.procedure_note_charge_for_pt_day_ind,
    stage.not_in_maintenance_dialysis_cohort_ind,
    max(stage.dialysis_before_maintenance_start_ind) as dialysis_before_maintenance_start_ind,
    max(stage.acute_dialysis_patient_day_ind) as acute_dialysis_patient_day_ind,
    /* Below indicator is for patient review
        Patients who have an esrd dx code on their problem list
        and excludes dates after maintenance dialysis start*/
    stage.esrd_problem_list_exclusion_ind
from
    stage
    inner join {{ ref('stg_patient')}} as stg_patient
        on stage.pat_key = stg_patient.pat_key
    left join esrd_exclusion
        on stage.pat_key = esrd_exclusion.pat_key
where
    age_years < 21
group by
    stage.submission_year,
    stage.metric_id,
    stg_patient.patient_name,
    stage.mrn,
    stage.visit_key,
    stage.note_department_name,
    stage.billing_department_name,
    stage.hospital_admit_date,
    stage.hospital_discharge_date,
    stage.date_of_interest,
    stage.procedure_note_for_pt_day_ind,
    stage.procedure_note_charge_for_pt_day_ind,
    stage.not_in_maintenance_dialysis_cohort_ind,
    stage.esrd_problem_list_exclusion_ind,
    stg_patient.dob
