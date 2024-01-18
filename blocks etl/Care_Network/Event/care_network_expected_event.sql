with secondary_provider as (
--region secondary_provider: will ensure we capture secondary provider NPI
    select
        pat_enc_appt.pat_enc_csn_id,
        pat_enc_appt.prov_id as secondary_provider_id,
        clarity_ser.prov_name as secondary_provider_name,
        clarity_ser.prov_type as secondary_provider_type,
        clarity_ser_2.npi as secondary_provider_npi
    from
        {{ source('clarity_ods', 'pat_enc_appt') }} as pat_enc_appt
        inner join {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser
            on clarity_ser.prov_id = pat_enc_appt.prov_id
        inner join {{ source('clarity_ods', 'clarity_ser_2') }} as clarity_ser_2
            on clarity_ser_2.prov_id = pat_enc_appt.prov_id
    where
        pat_enc_appt.line = 2
        and lower(clarity_ser.prov_type) != 'resource'
--end region
),
los_provider as (
--region secondary_provider: will ensure we capture los provider
    select
        pat_enc_disp.pat_enc_csn_id,
        pat_enc_disp.los_auth_prov_id as los_provider_id,
        clarity_ser.prov_name as los_provider_name,
        clarity_ser.prov_type as los_provider_type,
        clarity_ser_2.npi as los_provider_npi
    from
        {{ source('clarity_ods', 'pat_enc_disp') }} as pat_enc_disp
        inner join {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser
            on clarity_ser.prov_id = pat_enc_disp.los_auth_prov_id
        inner join {{ source('clarity_ods', 'clarity_ser_2') }} as clarity_ser_2
            on clarity_ser_2.prov_id = pat_enc_disp.los_auth_prov_id
    where
        lower(clarity_ser.prov_type) != 'resource'
--end region
),
erroneous_encounters as (
--region erroneous encounters
select
    diagnosis_encounter_all.visit_key,
    1 as erroneous_encounter_ind
from
    {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
where
    diagnosis_encounter_all.diagnosis_id in ('205946', '15079') --left without seen, erroneous encounter
group by
    diagnosis_encounter_all.visit_key
--end region erroneous encounter
)
select
    stg_encounter_care_network.visit_key,
    stg_encounter_care_network.csn,
    stg_encounter_care_network.encounter_date,
    pat_enc.appt_time as appointment_time,
    stg_encounter_care_network.pat_key,
    stg_encounter_care_network.mrn,
    stg_encounter_care_network.patient_name,
    stg_encounter_care_network.provider_ind,
    stg_encounter_care_network.provider_name as visit_provider, -- should thisrenamed to align with tdl?
    stg_encounter_care_network.provider_id as visit_provider_id,
    stg_encounter_care_network.prov_key,
    clarity_ser_2.npi as visit_provider_npi,
    stg_encounter_care_network.prov_type as visit_provider_type,
    los_provider.los_provider_name,
    los_provider.los_provider_id,
    los_provider.los_provider_type,
    los_provider.los_provider_npi,
    secondary_provider.secondary_provider_name,
    secondary_provider.secondary_provider_id,
    secondary_provider.secondary_provider_type,
    secondary_provider.secondary_provider_npi,
    stg_encounter_care_network.dept_key,
    department_care_network.specialty_name,
    stg_encounter_care_network.department_name,
    stg_encounter_care_network.department_id,
    stg_encounter_care_network.encounter_type_id,
    stg_encounter_care_network.encounter_type,
    stg_encounter_care_network.appointment_status,
    stg_encounter_care_network.appointment_status_id,
    stg_encounter_care_network.level_service_procedure_code,
    stg_encounter_care_network.visit_type,
    stg_encounter_care_network.visit_type_id,
    --should probably be moved to a main block
    coalesce(erroneous_encounters.erroneous_encounter_ind, 0) as erroneous_encounter_ind,

    --Encounter Closure
    case
        when pat_enc.enc_closed_yn = 'Y' then 1
        when pat_enc.enc_closed_yn = 'N' then 0
        else 0 --should we make this a -2 since appointments may need to be differentiated
    end as encounter_closed_ind,
    pat_enc.enc_close_date as encounter_closed_date,
    extract(days from (pat_enc.enc_close_date - stg_encounter_care_network.encounter_date))
        as encounter_days_to_close,

    --AVS
    case when pat_enc.avs_print_tm is not null then 1 else 0 end as after_visit_sum_ind,
    cast(pat_enc.avs_print_tm as date) as after_visit_sum_print_date,
    extract(epoch from(pat_enc.avs_print_tm - cast(pat_enc.appt_time as datetime))) / 3600.0
        as after_visit_sum_hours_to_print,

     --Allergies
    coalesce(stg_allergy_verification_events.allergies_verified_ind, 0) as allergies_verified_ind,
    cast(stg_allergy_verification_events.alrg_updt_dttm_encounter as date) as allergies_last_verified_date,
    coalesce(stg_allergy_verification_events.allergies_verified_in_encounter_ind, 0)
        as allergies_verified_in_encounter_ind,
    coalesce(stg_allergy_medication_event_expected.event_expected, 0) as allergies_verified_expected_ind,

     --Medications
    coalesce(stg_medication_review_events.medications_reviewed_ind, 0) as medications_reviewed_ind,
    cast(stg_medication_review_events.meds_hx_rev_instant_encounter as date) as medications_last_reviewed_date,
    coalesce(stg_medication_review_events.medications_reviewed_in_encounter_ind, 0)
        as medications_reviewed_in_encounter_ind,
    coalesce(stg_allergy_medication_event_expected.event_expected, 0) as medications_reviewed_expected_ind

from
    {{ ref('stg_encounter_care_network') }} as stg_encounter_care_network
    inner join {{ ref('department_care_network') }} as department_care_network
        on department_care_network.dept_key = stg_encounter_care_network.dept_key
    inner join {{ source('clarity_ods', 'clarity_ser_2') }} as clarity_ser_2
        on clarity_ser_2.prov_id = stg_encounter_care_network.provider_id
    inner join {{ source('clarity_ods','pat_enc') }} as pat_enc
        on pat_enc.pat_enc_csn_id = stg_encounter_care_network.csn
    left join secondary_provider as secondary_provider
        on secondary_provider.pat_enc_csn_id = stg_encounter_care_network.csn
    left join los_provider as los_provider
        on los_provider.pat_enc_csn_id = stg_encounter_care_network.csn
    left join erroneous_encounters as erroneous_encounters
        on stg_encounter_care_network.visit_key = erroneous_encounters.visit_key
    left join {{ ref('stg_allergy_verification_events') }} as stg_allergy_verification_events
        on stg_allergy_verification_events.csn = stg_encounter_care_network.csn
    left join {{ ref('stg_medication_review_events') }} as stg_medication_review_events
        on stg_medication_review_events.csn = stg_encounter_care_network.csn
    left join {{ ref('stg_allergy_medication_event_expected') }} as stg_allergy_medication_event_expected
        on stg_allergy_medication_event_expected.csn = stg_encounter_care_network.csn
