with first_icu_metrics as (
    --region when did patient first enter ICU
    select
        visit_key,
        min(enter_date) as first_icu_date
    from
        {{ref('adt_department_group')}}
    where
        --patient was in ICU unit in first 48 hours of admission
        bed_care_group in (
            'PHL ICU',
            'PHL ICU FLEX', --Overflow
            'KOPH ICU'
        )
        and initial_service = 'Critical Care'
    group by
        visit_key
)

--patient never entered ICU during admission and had one of the following antibiotics
select
    asp_ip_cap_cohort.visit_key,
    'non_icu_treatment_ind' as treatment_group,
    stg_asp_ip_cap_cohort_abx.abx_name as treatment_type
from
    {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    inner join {{ref('stg_asp_ip_cap_cohort_abx')}} as stg_asp_ip_cap_cohort_abx
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_cohort_abx.visit_key
    left join first_icu_metrics
        on asp_ip_cap_cohort.visit_key = first_icu_metrics.visit_key
        and first_icu_metrics.first_icu_date <= asp_ip_cap_cohort.inpatient_admit_date + interval('48 hours')
where
    first_icu_metrics.visit_key is null
    and lower(stg_asp_ip_cap_cohort_abx.abx_name) in (
        'penicillin g',
        'penicillin v',
        'ampicillin',
        'amoxicillin'
    )
    and stg_asp_ip_cap_cohort_abx.first_48_hrs_ind = 1
group by
    asp_ip_cap_cohort.visit_key,
    treatment_type

union all

--(ICU in first 48 hours or had Complicated Pneunmonia) and had one of the following antibiotics
select
    asp_ip_cap_cohort.visit_key,
    max(case when asp_ip_cap_cohort.complicated_pneumonia_ind = 1
        then 'complicated_pneumonia_treatment_ind' else 'icu_treatment_ind' end) as treatment_group,
    stg_asp_ip_cap_cohort_abx.abx_name as treatment_type
from
    {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    inner join {{ref('stg_asp_ip_cap_cohort_abx')}} as stg_asp_ip_cap_cohort_abx
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_cohort_abx.visit_key
    left join first_icu_metrics
        on asp_ip_cap_cohort.visit_key = first_icu_metrics.visit_key
where
    (
        --patient entered ICU within 48 hours of admission
        first_icu_metrics.first_icu_date < asp_ip_cap_cohort.hospital_admit_date + interval('48 hours')
        --patient has ICD10 code for Complicated Pneumonia
        or asp_ip_cap_cohort.complicated_pneumonia_ind = 1
    )
    and lower(stg_asp_ip_cap_cohort_abx.abx_name) in (
            'penicillin g',
            'penicillin v',
            'ampicillin',
            'amoxicillin',
            'cefotaxime',
            'ceftriaxone'
        )
        and stg_asp_ip_cap_cohort_abx.first_48_hrs_ind = 1
group by
    asp_ip_cap_cohort.visit_key,
    treatment_type

union all

--patient had Beta-Lactam allergy (any antibiotic acceptable)
select
    visit_key,
    'allergy_treatment_ind' as treatment_group,
    cast(group_concat(history_description) as varchar(100)) as treatment_type
from
    {{ref('stg_asp_ip_cap_metric_medical_history')}}
where
    history_type = 'Allergy'
    and active_visit_ind = 1
group by
    visit_key

union all

--patient had Beta-Lactam exposure in last 30 days (any antibiotic acceptable)
select
    visit_key,
    'beta_lactam_history_ind' as treatment_group,
    'Beta Lactam' as treatment_type
from
    {{ref('stg_asp_ip_cap_metric_medication_history')}}
where
    beta_lactam_use_30_day_ind = 1
group by
    visit_key,
    treatment_type

union all

--patient had positive MRSA culture in prior year and had anti-MRSA antibiotic
select
    stg_asp_ip_cap_metric_medical_history.visit_key,
    'mrsa_culture_treatment_ind' as treatment_group,
    stg_asp_ip_cap_cohort_abx.abx_name as treatment_type
from
    {{ref('stg_asp_ip_cap_metric_medical_history')}} as stg_asp_ip_cap_metric_medical_history
    inner join {{ref('stg_asp_ip_cap_cohort_abx')}} as stg_asp_ip_cap_cohort_abx
        on stg_asp_ip_cap_metric_medical_history.visit_key = stg_asp_ip_cap_cohort_abx.visit_key
where
    stg_asp_ip_cap_metric_medical_history.history_type = 'Culture'
    and stg_asp_ip_cap_metric_medical_history.active_visit_ind = 1
    and regexp_like(
        stg_asp_ip_cap_metric_medical_history.history_description,
        'mrsa|staph.*aur|methicillin'
    )
    and lower(stg_asp_ip_cap_cohort_abx.abx_name) in (
        'vancomycin',
        'linezolid',
        'clindamycin',
        'sulfamethoxazole trimethoprim',
        'ceftaroline'
    )
group by
    stg_asp_ip_cap_metric_medical_history.visit_key,
    treatment_type

union all

--patient only prescribed Azithromycin and (older than 5 yrs old or had positive PCR testing)
select
    stg_asp_ip_cap_metric_medication_history.visit_key,
    'azithromycin_monotherapy_treatment_ind' as treatment_group,
    'Azithromycin' as treatment_type
from
    {{ref('stg_asp_ip_cap_metric_medication_history')}} as stg_asp_ip_cap_metric_medication_history
    left join {{ref('stg_asp_ip_cap_metric_medical_history')}} as stg_asp_ip_cap_metric_medical_history
        on stg_asp_ip_cap_metric_medication_history.visit_key = stg_asp_ip_cap_metric_medical_history.visit_key
where
    stg_asp_ip_cap_metric_medication_history.azithromycin_monotherapy_ind = 1
    and (
        stg_asp_ip_cap_metric_medication_history.age_years > 5
        or (
            stg_asp_ip_cap_metric_medical_history.history_type = 'Culture'
            and stg_asp_ip_cap_metric_medical_history.active_visit_ind = 1
            and stg_asp_ip_cap_metric_medical_history.history_description = 'mycoplasma'
        )
    )
group by
    stg_asp_ip_cap_metric_medication_history.visit_key,
    treatment_type
