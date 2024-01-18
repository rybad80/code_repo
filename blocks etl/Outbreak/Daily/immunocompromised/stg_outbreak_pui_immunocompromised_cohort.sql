{{ config(materialized='table', dist='pat_key') }}

select
    stg_outbreak_covid_pui_cohort.pat_key,
    stg_outbreak_covid_pui_cohort.min_specimen_taken_date,
    'covid' as outbreak_type
from
    {{ref('stg_outbreak_covid_pui_cohort')}} as stg_outbreak_covid_pui_cohort
group by
    stg_outbreak_covid_pui_cohort.pat_key,
    stg_outbreak_covid_pui_cohort.min_specimen_taken_date
union all
select
    stg_outbreak_flu_pui_cohort.pat_key,
    stg_outbreak_flu_pui_cohort.min_specimen_taken_date,
    stg_outbreak_flu_pui_cohort.test_type as outbreak_type
from
    {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
group by
    stg_outbreak_flu_pui_cohort.pat_key,
    stg_outbreak_flu_pui_cohort.min_specimen_taken_date,
    stg_outbreak_flu_pui_cohort.test_type
