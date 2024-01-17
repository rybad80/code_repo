with arrest_metrics as (
    select
        cardiac_unit_encounter.visit_key,
        cardiac_unit_encounter.department_admit_date,
        max(case when cardiac_arrest_all.arrest_date is not null
                then 1 else 0 end) as arrest_ind,
        max(case when date(stg_cardiac_pc4_mortality.r_mort_dt)
            >= date(cardiac_unit_encounter.hospital_admit_date)
            and date(stg_cardiac_pc4_mortality.r_mort_dt)
            <= date(cardiac_unit_encounter.hospital_discharge_date)
            then 1 else 0 end) as visit_death_ind
    from
        {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
        inner join {{ref('stg_cardiac_pc4_mortality')}} as stg_cardiac_pc4_mortality
                    on stg_cardiac_pc4_mortality.pat_key = cardiac_unit_encounter.pat_key
        left join {{ref('cardiac_arrest_all')}}  as cardiac_arrest_all
            on cardiac_arrest_all.enc_key = cardiac_unit_encounter.enc_key
    group by
        cardiac_unit_encounter.visit_key,
        cardiac_unit_encounter.department_admit_date
)

select
    cardiac_unit_encounter.mrn,
    cardiac_unit_encounter.pat_key,
    cardiac_unit_encounter.dob,
    cardiac_unit_encounter.visit_key,
    cardiac_unit_encounter.hospital_admit_date,
    cardiac_unit_encounter.hospital_discharge_date,
    cardiac_unit_encounter.department_admit_date as cicu_start_date,
    cardiac_unit_encounter.department_discharge_date as cicu_end_date,
    date(cardiac_unit_encounter.department_admit_date)
        - date(cardiac_unit_encounter.dob) as admit_age_days,
    case when admit_age_days <= 30
        then 1
        else 0 end as neonate_ind,
    {{
        dbt_utils.surrogate_key([
            'cardiac_unit_encounter.visit_key',
            'cicu_start_date'
            ])
    }} as cicu_enc_key,
    cardiac_unit_encounter.department_los_days as cicu_los_days,
    arrest_metrics.arrest_ind,
    arrest_metrics.visit_death_ind

from
    {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
    inner join arrest_metrics
        on arrest_metrics.visit_key = cardiac_unit_encounter.visit_key
        and arrest_metrics.department_admit_date = cardiac_unit_encounter.department_admit_date
where
    cardiac_unit_encounter.department_admit_date >= '01-01-2015'
    and lower(cardiac_unit_encounter.department_name) = 'cicu'
