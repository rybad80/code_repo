with patients as (
select
    stg_patient_ods.patient_key,
    stg_patient_ods.mrn,
    stg_patient_ods.pat_id,
    first_value(
        stg_encounter_outpatient.encounter_key
        ) over (
            partition by stg_encounter_outpatient.patient_key
            order by encounter_date asc,
                appointment_date asc
    ) as first_encounter_key,
    first_value(
        stg_encounter_outpatient.encounter_key
        ) over (
            partition by stg_encounter_outpatient.patient_key
            order by encounter_date desc,
                appointment_date desc
    ) as last_encounter_key
from {{ref('stg_patient_ods')}} as stg_patient_ods
    inner join {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
        on stg_encounter_outpatient.patient_key = stg_patient_ods.patient_key
            and stg_encounter_outpatient.primary_care_ind = 1
            -- not applicable, completed, arrived
            and stg_encounter_outpatient.appointment_status_id in (-2, 2, 6)
where stg_encounter_outpatient.encounter_date < current_date
)
select
    patient_key,
    mrn,
    pat_id,
    first_encounter_key,
    last_encounter_key
from patients
group by
    patient_key,
    mrn,
    pat_id,
    first_encounter_key,
    last_encounter_key
