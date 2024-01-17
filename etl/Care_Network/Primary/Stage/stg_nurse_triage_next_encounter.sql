{{ config(meta = {
    'critical': true
}) }}

with next_sick as (
select
    stg_encounter_nurse_triage.encounter_key,
    stg_encounter_nurse_triage.visit_key,
    stg_encounter_nurse_triage.patient_key,
    stg_encounter_nurse_triage.pat_key,
    stg_encounter_nurse_triage.encounter_date,
    stg_encounter_nurse_triage.encounter_instant,
    stg_encounter_outpatient.encounter_key as next_sick_encounter_key,
    stg_encounter_outpatient.encounter_date as next_sick_encounter_date,
    stg_encounter_outpatient.appointment_date as next_sick_appointment_date,
    stg_encounter_outpatient.department_id as next_sick_department_id,
    stg_encounter_outpatient.department_name as next_sick_department_name,
    stg_encounter_outpatient.provider_id as next_sick_provider_id,
    stg_encounter_outpatient.provider_name as next_sick_provider_name,
    row_number() over (
        partition by stg_encounter_nurse_triage.patient_key
        order by stg_encounter_outpatient.encounter_date asc,
            stg_encounter_outpatient.appointment_date asc,
            stg_encounter_outpatient.start_visit_date asc
    ) as visit_num
from {{ ref('stg_encounter_nurse_triage') }} as stg_encounter_nurse_triage
    inner join {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
        on stg_encounter_outpatient.patient_key = stg_encounter_nurse_triage.patient_key
            and (stg_encounter_outpatient.appointment_date > stg_encounter_nurse_triage.encounter_instant
            or (stg_encounter_outpatient.appointment_date is null
                and stg_encounter_outpatient.encounter_date >= stg_encounter_nurse_triage.encounter_date))
where
    lower(stg_encounter_outpatient.intended_use_name) = 'primary care'
    and (lower(stg_encounter_outpatient.visit_type) like '%sick%'
        or stg_encounter_outpatient.sick_visit_ind = 1)
    -- only scheduled, completed, arrived encounters
    and stg_encounter_outpatient.appointment_status_id in (1, 2, 6)
)

select
    encounter_key,
    visit_key,
    patient_key,
    pat_key,
    encounter_date,
    encounter_instant,
    next_sick_encounter_key,
    next_sick_encounter_date,
    next_sick_appointment_date,
    next_sick_department_id,
    next_sick_department_name,
    next_sick_provider_id,
    next_sick_provider_name,
    days_between(encounter_date, next_sick_encounter_date) as next_sick_lag_days
from next_sick
where visit_num = 1
