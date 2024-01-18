with reasons as (
    select
        visit_reason.visit_key,
        master_reason_for_visit.rsn_disp_nm as reason_name
    from
        {{source('cdw', 'visit_reason')}} as visit_reason
        inner join {{source('cdw', 'master_reason_for_visit')}} as master_reason_for_visit
            on master_reason_for_visit.rsn_key = visit_reason.rsn_key
    where
        visit_reason.seq_num = 1
)

select
    encounter_all.visit_key,
    encounter_all.mrn,
    encounter_all.patient_name,
    encounter_all.csn,
    encounter_all.dob,
    encounter_all.age_years,
    encounter_all.sex,
    stg_patient.mailing_address_line1,
    stg_patient.mailing_address_line2,
    stg_patient.mailing_city,
    stg_patient.mailing_state,
    stg_patient.mailing_zip,
    stg_patient.county,
    patient.home_ph as home_phone_number,
    encounter_primary_care.provider_name as primary_care_provider,
    encounter_all.provider_name,
    coalesce(
        encounter_primary_care.appointment_made_date,
        encounter_specialty_care.appointment_made_date
    ) as appointment_made_date,
    encounter_all.appointment_status,
    coalesce(encounter_primary_care.check_in_date, encounter_specialty_care.check_in_date) as check_in_date,
    case
        when encounter_primary_care.check_in_date is not null
            then round(extract( --noqa: PRS
                epoch from encounter_primary_care.done_rooming_date - encounter_primary_care.start_rooming_date
            ) / 60.0 / 60, 2)
        when encounter_specialty_care.check_in_date is not null
            then round(extract( --noqa: PRS
                epoch from encounter_primary_care.done_rooming_date - encounter_primary_care.start_rooming_date
            ) / 60.0 / 60, 2)
    end as time_in_room,
    reasons.reason_name,
    encounter_all.pat_key
from
    {{ref('encounter_all')}} as encounter_all
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = encounter_all.pat_key
    inner join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = stg_patient.pat_key
    left join {{ref('encounter_primary_care')}} as encounter_primary_care
        on encounter_primary_care.visit_key = encounter_all.visit_key
    left join {{ref('encounter_specialty_care')}} as encounter_specialty_care
        on encounter_specialty_care.visit_key = encounter_all.visit_key
    left join reasons
        on reasons.visit_key = encounter_all.visit_key
where
    encounter_all.appointment_status_id not in (1, 3, 4) /* scheduled, canceled, no-show */
    or encounter_all.encounter_date <= current_date
