select
    scheduling_specialty_care_appointments.visit_key,
    scheduling_specialty_care_appointments.pat_key,
    scheduling_specialty_care_appointments.encounter_date,
    date(scheduling_specialty_care_appointments.encounter_date) - date(stg_patient.dob)
        as actual_age_in_days_at_encounter,
    actual_age_in_days_at_encounter / 30.5 as actual_age_in_months_at_encounter,
    case
        when stg_patient.gestational_age_complete_weeks >= 37
        then actual_age_in_months_at_encounter
        else (
            actual_age_in_days_at_encounter
            + stg_patient.gestational_age_complete_weeks * 7
            + stg_patient.gestational_age_remainder_days
        ) / 30.5 - 40.0 * 7.0 / 30.5  /* convert 40 weeks to months */
    end as corrected_age_in_months_at_encounter,
    case
        when lower(scheduling_specialty_care_appointments.appointment_status) = 'no show' then 1
        when cancel_48hr_ind = 1 then 1
        else 0
    end as no_show_or_cancel_48h_ind,
    scheduling_specialty_care_appointments.appointment_status_id,
    scheduling_specialty_care_appointments.past_appointment_ind,
    case
        /* abington neonatal fol */
        when scheduling_specialty_care_appointments.department_id = 101012064 then 'abington'
        /* bgr neonatal fol */
        when scheduling_specialty_care_appointments.department_id = 101012123 then 'buerger'
        /* ext neonatal fol */
        when scheduling_specialty_care_appointments.department_id = 80368021 then 'ext'
        /* kop neonatal fol */
        when scheduling_specialty_care_appointments.department_id = 101012161 then 'kop'
        /* lgh neonatal fol */
        when scheduling_specialty_care_appointments.department_id = 101012160 then 'lgh'
        /* pennsylvania neonatology */
        when scheduling_specialty_care_appointments.department_id = 101012052 then 'pennsy'
        /* virtua neonatology */
        when scheduling_specialty_care_appointments.department_id = 101022013 then 'virtua'
        /* wood neonatal fol */
        when scheduling_specialty_care_appointments.department_id = 89369021 then 'wood'
    end as visit_location,
    row_number() over (
        partition by scheduling_specialty_care_appointments.pat_key
        order by scheduling_specialty_care_appointments.encounter_date asc
    ) as visit_number


from
    {{ ref('scheduling_specialty_care_appointments') }} as scheduling_specialty_care_appointments
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = scheduling_specialty_care_appointments.pat_key

where
    lower(scheduling_specialty_care_appointments.specialty_name) = 'neonatology'
    and scheduling_specialty_care_appointments.department_id not in (
        101012046, /* main neonatology consult */
        10801021  /* mkt 3550 spec babies */
    )
    and not lower(scheduling_specialty_care_appointments.appointment_status) = 'not applicable'
