with fs_intubations as (
    --get all Endotracheal Tubes from Flowsheet LDA table
    select
        flowsheet_lda.flowsheet_rec_visit_key,
        flowsheet_lda.ip_lda_id as lda_id,
        flowsheet_lda.placement_instant as actual_intubation_datetime,
        case
            when flowsheet_lda.removal_instant != '2157-11-19 17:46:00' then flowsheet_lda.removal_instant
        end as actual_extubation_datetime
    from
        {{ ref('flowsheet_lda') }} as flowsheet_lda
    where
        lower(flowsheet_lda.lda_description) like '%endotracheal tube%'
        and year(coalesce(flowsheet_lda.removal_instant, current_date)) >= 2023
    group by
        flowsheet_lda.flowsheet_rec_visit_key,
        flowsheet_lda.ip_lda_id,
        flowsheet_lda.placement_instant,
        flowsheet_lda.removal_instant
),

vps_data as (
    select
        picu_episode_cohort.vps_episode_key,
        picu_episode_cohort.visit_key,
        picu_episode_cohort.mrn,
        picu_episode_cohort.patient_name,
        picu_episode_cohort.csn,
        picu_episode_cohort.dob,
        1 as type_icu,
        picu_episode_cohort.case_id,
        picu_episode_cohort.picu_admit_date as icu_start_datetime,
        picu_episode_cohort.picu_medical_discharge_date as icu_end_datetime,
        case
            when picu_episode_cohort.legal_sex = 'Male' then 1
            when picu_episode_cohort.legal_sex = 'Female' then 2
        end as sex,
        picu_episode_cohort.race,
        picu_episode_cohort.ethnicity,
        picu_episode_cohort.picu_disposition as icu_disposition,
        round(picu_episode_cohort.pim3_rom, 2) as pim3,
        round(picu_episode_cohort.medical_los_days * 24, 1) as icu_los,
        fs_intubations.lda_id,
        fs_intubations.actual_intubation_datetime,
        --ICU constrained intubation time
        case
            when fs_intubations.actual_intubation_datetime < picu_episode_cohort.picu_admit_date
                then picu_episode_cohort.picu_admit_date
            else fs_intubations.actual_intubation_datetime
        end as intubation_datetime,
        fs_intubations.actual_extubation_datetime,
        --death constrained extubation time
        case
            when coalesce(fs_intubations.actual_extubation_datetime, current_date)
                > coalesce(stg_patient.death_date, current_date)
                then stg_patient.death_date
            else fs_intubations.actual_extubation_datetime
        end as extubation_datetime,
        case
            when intubation_datetime is not null then intubation_datetime + interval '24 hours'
            else picu_episode_cohort.picu_admit_date + interval '24 hours'
        end as enrollment_date,
        extract(
            epoch from enrollment_date - picu_episode_cohort.dob
        ) / 86400 as age_days,
        --find the closest ICU admission associated with each lda
        row_number() over (
            partition by fs_intubations.lda_id
            order by abs(extract(
                epoch from picu_episode_cohort.picu_admit_date - fs_intubations.actual_intubation_datetime))
        ) as closest_icu_admission
    from
        {{ ref('picu_episode_cohort') }} as picu_episode_cohort
        inner join fs_intubations
            on picu_episode_cohort.visit_key = fs_intubations.flowsheet_rec_visit_key
        inner join {{ ref('stg_patient') }} as stg_patient
            on picu_episode_cohort.pat_key = stg_patient.pat_key
    where
        --intubations before ICU discharge
        coalesce(fs_intubations.actual_intubation_datetime, picu_episode_cohort.picu_admit_date)
            <= coalesce(picu_episode_cohort.picu_medical_discharge_date, current_date)
        --extubations after ICU admission
        and coalesce(fs_intubations.actual_extubation_datetime, current_date)
            >= picu_episode_cohort.picu_admit_date
)

select
    vps_data.vps_episode_key,
    vps_data.visit_key,
    vps_data.mrn,
    vps_data.patient_name,
    vps_data.csn,
    vps_data.dob,
    vps_data.type_icu,
    vps_data.case_id,
    vps_data.icu_start_datetime,
    vps_data.icu_end_datetime,
    vps_data.lda_id,
    vps_data.intubation_datetime,
    vps_data.extubation_datetime,
    vps_data.enrollment_date,
    vps_data.age_days,
    vps_data.sex,
    vps_data.race,
    vps_data.ethnicity,
    vps_data.icu_disposition,
    vps_data.pim3,
    vps_data.icu_los,
    case when vps_data.extubation_datetime is not null then 1 end as any_extubation_attempt,
    stg_ventilator_liberation_redcap.local_encounter_id,
    --create instance # for intubation if part of same ICU encounter
    row_number() over (
        partition by vps_data.visit_key, vps_data.icu_start_datetime
        order by vps_data.actual_intubation_datetime
    ) as redcap_repeat_instance,
    stg_ventilator_liberation_redcap.multicenter_encounter,
    stg_ventilator_liberation_redcap.qualify_transfer
from
    vps_data
    left join {{ ref('stg_ventilator_liberation_redcap') }} as stg_ventilator_liberation_redcap
        on vps_data.lda_id = stg_ventilator_liberation_redcap.lda_id
where
    vps_data.closest_icu_admission = 1
    and vps_data.enrollment_date <= coalesce(vps_data.actual_extubation_datetime, current_date)
    and vps_data.enrollment_date <= coalesce(vps_data.icu_end_datetime, current_date)
    and vps_data.enrollment_date >= '2023-10-15'
