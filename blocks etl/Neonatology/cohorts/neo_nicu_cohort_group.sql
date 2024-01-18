with cohorts as (
    select
        pat_key,
        cohort_group_name,
        cohort_group_display_name,
        cohort_group_enter_date
    from
        {{ ref('stg_neo_nicu_diagnosis_group') }}

    union all

    select
        pat_key,
        cohort_group_name,
        cohort_group_display_name,
        cohort_group_enter_date
    from
        {{ ref('stg_neo_nicu_surgery_group') }}

    union all

    select
        pat_key,
        'bpd' as cohort_group_name,
        'BPD' as cohort_group_display_name,
        bpd_index_date as cohort_group_enter_date
    from
        {{ ref('stg_neo_nicu_bpd_group') }}

    union all

    select
        pat_key,
        'history of ecmo' as cohort_group_name,
        'History of ECMO' as cohort_group_display_name,
        date(min(ecmo_start_datetime)) as cohort_group_enter_date
    from
        {{ ref('flowsheet_ecmo') }}
    where
        lower(department_group_name_at_ecmo_start) = 'nicu'
    group by
        pat_key,
        cohort_group_name,
        cohort_group_display_name

    union all

    select
        pat_key,
        'lymphatics' as cohort_group_name,
        'Lymphatics' as cohort_group_display_name,
        cohort_group_enter_date
    from
        {{ ref('stg_neo_nicu_lymphatics_group') }}
)

select
    stg_patient.pat_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.sex,
    stg_patient.gestational_age_complete_weeks,
    stg_patient.gestational_age_remainder_days,
    floor(stg_patient.birth_weight_kg * 1000) as birth_weight_grams,
    cohorts.cohort_group_name,
    cohorts.cohort_group_display_name,
    cohorts.cohort_group_enter_date
from
    {{ ref('stg_patient') }} as stg_patient
    inner join cohorts
        on cohorts.pat_key = stg_patient.pat_key
