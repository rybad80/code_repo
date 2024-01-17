with
age_base as (--region
    select
        'Age' as metric_name,
        case
            when stg_patient.current_age < 1 then 'Infant'
            when stg_patient.current_age < 2 then 'Toddler'
            when stg_patient.current_age < 5 then 'Early Chidhood'
            when stg_patient.current_age < 11 then 'Middle Childhood'
            when stg_patient.current_age < 18 then 'Adolescence'
            when stg_patient.current_age >= 18 then 'Adulthood'
            else 'no age' end as metric_level,
        count(distinct frontier_hi_encounter_cohort.pat_key) as metric_value
    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('stg_patient')}} as stg_patient on frontier_hi_encounter_cohort.mrn = stg_patient.mrn
    group by
        metric_level
--end region
),
sex_base as (--region
    select
        'Sex' as metric_name,
        case
            when sex = 'M' then 'Male'
            when sex = 'F' then 'Female'
            when sex = 'U' then 'Unknown'
            else 'not recorded' end
            as metric_level,
        count(distinct frontier_hi_encounter_cohort.pat_key) as metric_value
    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('stg_patient')}} as stg_patient on frontier_hi_encounter_cohort.mrn = stg_patient.mrn
    group by
        metric_level

    --end region
),
race_base as (--region: sub race base
    select
        'Race' as metric_name,
        race as metric_level,
        count(distinct frontier_hi_encounter_cohort.pat_key) as metric_value
    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('stg_patient')}} as stg_patient on frontier_hi_encounter_cohort.mrn = stg_patient.mrn
    where
        metric_level is not null
    group by
        metric_level
    --end region
),
race_ethnicity_base as (--region: sub ethnicity base
    select
        'Race Ethnicity' as metric_name,
        race_ethnicity as metric_level,
        count(distinct frontier_hi_encounter_cohort.pat_key) as metric_value
    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('stg_patient')}} as stg_patient on frontier_hi_encounter_cohort.mrn = stg_patient.mrn
    where
        metric_level is not null
    group by
        metric_level
    --end region
),
ethnicity_base as (--region
    select
        'Ethnicity' as metric_name,
        ethnicity as metric_level,
        count(distinct frontier_hi_encounter_cohort.pat_key) as metric_value
    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('stg_patient')}} as stg_patient on frontier_hi_encounter_cohort.mrn = stg_patient.mrn
    where
        metric_level is not null
    group by
        metric_level
    --end region
),
final_union as (--region
        select * from age_base
        union all
        select * from sex_base
        union all
        select * from race_base
        union all
        select * from race_ethnicity_base
        union all
        select * from ethnicity_base
    --end region
)
select * from final_union
--;
