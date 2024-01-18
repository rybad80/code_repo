{{ config(meta = {
    'critical': true
}) }}

with chnd_adt as (
    select
        row_number() over (
            order by neo_nicu_episode_phl.mrn,
                neo_nicu_episode_phl.hospital_admit_date
            ) as rn,
        neo_nicu_episode_phl.mrn,
        neo_nicu_episode_phl.patient_name,
        date(neo_nicu_episode_phl.dob) as date_of_birth,
        time(neo_nicu_episode_phl.dob) as time_of_birth,
        neo_nicu_episode_phl.gestational_age_complete_weeks,
        neo_nicu_episode_phl.gestational_age_remainder_days,
        neo_nicu_episode_phl.birth_weight_grams,
        neo_nicu_episode_phl.sex,
        date(neo_nicu_episode_phl.hospital_admit_date) as hospital_admit_date,
        time(neo_nicu_episode_phl.hospital_admit_date) as hospital_admit_time,
        date(neo_nicu_episode_phl.episode_start_date) as episode_start_date,
        time(neo_nicu_episode_phl.episode_start_date) as episode_start_time,
        date(neo_nicu_episode_phl.episode_end_date) as episode_end_date,
        time(neo_nicu_episode_phl.episode_end_date) as episode_end_time,
        neo_nicu_episode_phl.nicu_disposition,
        neo_nicu_episode_phl.episode_start_treatment_team,
        neo_nicu_episode_phl.hospital_admission_source,
        neo_nicu_episode_phl.itcu_transfer_ind,
        encounter_inpatient.patient_address_zip_code,
        hospital_account.hsp_acct_id
    from
        {{ ref('neo_nicu_episode_phl') }} as neo_nicu_episode_phl
        left join {{ ref('encounter_inpatient') }} as encounter_inpatient
            on encounter_inpatient.visit_key = neo_nicu_episode_phl.visit_key
        left join {{source('cdw', 'hospital_account')}} as hospital_account
            on hospital_account.hsp_acct_key = encounter_inpatient.hsp_acct_key
)
select
    {{
        dbt_utils.surrogate_key([
            'mrn',
            'hospital_admit_date',
            'rn'
            ])
        }} as chnd_adt_key,
    mrn,
    patient_name,
    date_of_birth,
    time_of_birth,
    gestational_age_complete_weeks,
    gestational_age_remainder_days,
    birth_weight_grams,
    sex,
    hospital_admit_date,
    hospital_admit_time,
    episode_start_date,
    episode_start_time,
    episode_end_date,
    episode_end_time,
    nicu_disposition,
    episode_start_treatment_team,
    hospital_admission_source,
    itcu_transfer_ind,
    patient_address_zip_code,
    hsp_acct_id
from
    chnd_adt
