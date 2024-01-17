with azithromycin_use as (
    --did patient ONLY receive azithromycin?
    select
        asp_ip_cap_cohort.visit_key,
        asp_ip_cap_cohort.age_years,
        min(case when lower(stg_asp_ip_cap_cohort_abx.abx_name) = 'azithromycin'
            then 1 else 0 end) as azithromycin_monotherapy_ind
    from
        {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
        inner join {{ref('stg_asp_ip_cap_cohort_abx')}} as stg_asp_ip_cap_cohort_abx
            on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_cohort_abx.visit_key
    group by
        asp_ip_cap_cohort.visit_key,
        asp_ip_cap_cohort.age_years
),

beta_lactam_use_30_days as (
    select
        asp_ip_cap_cohort.visit_key,
        1 as beta_lactam_use_30_day_ind
    from
        {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
        inner join {{ref('stg_asp_abx_all')}} as stg_asp_abx_all
            on asp_ip_cap_cohort.pat_key = stg_asp_abx_all.pat_key
    where
        stg_asp_abx_all.order_mode = 'Inpatient'
        and asp_ip_cap_cohort.hospital_admit_date between
            stg_asp_abx_all.administration_date
            and stg_asp_abx_all.administration_date + interval('30 days')
        --Beta-Lactams
        and lower(stg_asp_abx_all.drug_class) in (
            'cephalosporins',
            'b-lactam/ b-lactamase inhibitor combination',
            'penicillins',
            'carbapenems',
            'monobactams'
        )
    group by
        asp_ip_cap_cohort.visit_key

    union all

    select
        asp_ip_cap_cohort.visit_key,
        1 as beta_lactam_use_30_day_ind
    from
        {{ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
        inner join {{ref('stg_asp_abx_all')}} as stg_asp_abx_all
            on asp_ip_cap_cohort.pat_key = stg_asp_abx_all.pat_key
    where
        stg_asp_abx_all.order_mode = 'Outpatient'
        and asp_ip_cap_cohort.hospital_admit_date between
            stg_asp_abx_all.medication_start_date
            and stg_asp_abx_all.medication_start_date + interval('30 days')
        --Beta-Lactams
        and lower(stg_asp_abx_all.drug_class) in (
            'cephalosporins',
            'b-lactam/ b-lactamase inhibitor combination',
            'penicillins',
            'carbapenems',
            'monobactams'
        )
    group by
        asp_ip_cap_cohort.visit_key
)

select
    azithromycin_use.visit_key,
    azithromycin_use.age_years,
    coalesce(azithromycin_use.azithromycin_monotherapy_ind, 0) as azithromycin_monotherapy_ind,
    coalesce(beta_lactam_use_30_days.beta_lactam_use_30_day_ind, 0) as beta_lactam_use_30_day_ind
from
    azithromycin_use
    left join beta_lactam_use_30_days
        on azithromycin_use.visit_key = beta_lactam_use_30_days.visit_key
group by
    azithromycin_use.visit_key,
    azithromycin_use.age_years,
    azithromycin_use.azithromycin_monotherapy_ind,
    beta_lactam_use_30_days.beta_lactam_use_30_day_ind
