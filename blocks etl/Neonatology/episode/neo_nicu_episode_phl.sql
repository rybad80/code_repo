{{ config(materialized= 'view' ) }}

select
    episode_key,
    patient_name,
    mrn,
    dob,
    sex,
    gestational_age_complete_weeks,
    gestational_age_remainder_days,
    birth_weight_grams,
    hospital_admit_date,
    hospital_discharge_date,
    episode_start_date,
    episode_end_date,
    nicu_disposition,
    nicu_los_days,
    nicu_episode_number,
    episode_start_treatment_team,
    episode_end_treatment_team,
    hospital_los_days,
    hospital_admission_source,
    inborn_ind,
    discharge_disposition,
    itcu_transfer_ind,
    visit_key,
    pat_key
from
    {{ ref('neo_nicu_episode') }}
where
    phl_icu_ind = 1
