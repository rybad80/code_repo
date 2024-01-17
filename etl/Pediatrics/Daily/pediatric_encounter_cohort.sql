{{ config(meta = {
    'critical': true
}) }}

with visit_all_indicators as (
    select
        stg_encounter.visit_key,
        stg_encounter.pat_key,
        stg_encounter.mrn,
        stg_encounter.patient_name,
        coalesce(stg_pediatric_encounter_cohort_diabetes.diabetes_ind, 0) as diabetes_ind,
        coalesce(stg_pediatric_ecounter_cohort_bp_under_3.bp_under_3_ind, 0) as bp_under_3_ind,
        coalesce(stg_pediatric_encounter_cohort_elect.elect_ind, 0) as elect_ind,
        coalesce(stg_pediatric_encounter_cohort_jia.jia_ind, 0) as jia_ind,
        coalesce(
            stg_pediatric_encounter_cohort_diabetes.diabetes_ind,
            stg_pediatric_ecounter_cohort_bp_under_3.bp_under_3_ind,
            stg_pediatric_encounter_cohort_elect.elect_ind,
            stg_pediatric_encounter_cohort_jia.jia_ind,
            0 ) as any_cohort_ind
            /*add all future indicators to any_ind*/
    from
        {{ref('stg_encounter')}} as stg_encounter
        left join {{ref('stg_pediatric_encounter_cohort_diabetes')}} as stg_pediatric_encounter_cohort_diabetes
            on stg_encounter.visit_key = stg_pediatric_encounter_cohort_diabetes.visit_key
        left join {{ref('stg_pediatric_encounter_cohort_bp_under_3')}} as stg_pediatric_ecounter_cohort_bp_under_3
            on stg_encounter.visit_key = stg_pediatric_ecounter_cohort_bp_under_3.visit_key
        left join {{ref('stg_pediatric_encounter_cohort_elect')}} as stg_pediatric_encounter_cohort_elect
            on stg_encounter.visit_key = stg_pediatric_encounter_cohort_elect.visit_key
        left join {{ref('stg_pediatric_encounter_cohort_jia')}} as stg_pediatric_encounter_cohort_jia
            on stg_encounter.visit_key = stg_pediatric_encounter_cohort_jia.visit_key
)
select *
from
    visit_all_indicators
where
    any_cohort_ind = 1
