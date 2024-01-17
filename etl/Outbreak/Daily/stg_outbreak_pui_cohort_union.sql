{{ config(materialized='table', dist='pat_key') }}

with cohort_union as (
    select distinct
        stg_outbreak_covid_pui_cohort.pat_key,
        stg_outbreak_covid_pui_cohort.min_specimen_taken_date,
        'covid' as outbreak_type
    from
        {{ref('stg_outbreak_covid_pui_cohort')}} as stg_outbreak_covid_pui_cohort
    union all
    select distinct
        stg_outbreak_flu_pui_cohort.pat_key,
        stg_outbreak_flu_pui_cohort.min_specimen_taken_date,
        stg_outbreak_flu_pui_cohort.test_type as outbreak_type
    from
        {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
)

select
    cohort_union.pat_key,
    cohort_union.outbreak_type,
    cohort_union.min_specimen_taken_date,
    cohort_union.min_specimen_taken_date - interval '30 days' as min_specimen_taken_date_pre30d,
    cohort_union.min_specimen_taken_date + interval '30 days' as min_specimen_taken_date_post30d
from
    cohort_union
