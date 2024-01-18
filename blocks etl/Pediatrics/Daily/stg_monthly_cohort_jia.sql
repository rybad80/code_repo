{{ config(meta = {
    'critical': false
}) }}

with base as (--Pull Rheumatology JIA cohort
    select
        stg_pediatric_encounter_cohort_jia.visit_key,
        stg_pediatric_encounter_cohort_jia.pat_key,
        stg_pediatric_encounter_cohort_jia.mrn,
        stg_pediatric_encounter_cohort_jia.csn,
        stg_pediatric_encounter_cohort_jia.encounter_date,
        stg_pediatric_encounter_cohort_jia.jia_ind
    from
        {{ref('stg_pediatric_encounter_cohort_jia')}} as stg_pediatric_encounter_cohort_jia
),

monthyear as ( --Apply cJADAS score to future months until a new score is entered or 15 months pass
    select
        base.visit_key,
        base.pat_key,
        base.mrn,
        base.csn,
        base.encounter_date,
        base.jia_ind,
        date_trunc('month', master_date.full_dt) as monthyear,
        last_day(monthyear) as pat_population_review_dt
    from
        base
        inner join {{source('cdw', 'master_date')}} as master_date
            on master_date.full_dt > base.encounter_date
    where
        extract(days from (monthyear - base.encounter_date)) <= 450 --score is updated annually w/ 3 month buffer
    group by
        base.visit_key,
        base.pat_key,
        base.mrn,
        base.csn,
        base.encounter_date,
        base.jia_ind,
        monthyear
),

pat_pop_sequence as (
    select
        monthyear.visit_key,
        monthyear.pat_key,
        monthyear.mrn,
        monthyear.csn,
        monthyear.encounter_date,
        monthyear.jia_ind,
        monthyear.monthyear,
        monthyear.pat_population_review_dt,
        --take the later survey for months with multiple surveys taken
        row_number() over (partition by monthyear.pat_key, monthyear.pat_population_review_dt
            order by monthyear.encounter_date desc) as pat_pop_period_seq
    from
        monthyear
)

select
    pat_pop_sequence.visit_key || pat_pop_sequence.pat_population_review_dt as jia_id,
    pat_pop_sequence.visit_key,
    pat_pop_sequence.pat_key,
    pat_pop_sequence.mrn,
    pat_pop_sequence.csn,
    pat_pop_sequence.encounter_date,
    pat_pop_sequence.jia_ind,
    pat_pop_sequence.monthyear,
    pat_pop_sequence.pat_population_review_dt
from
    pat_pop_sequence
where
    pat_pop_sequence.pat_pop_period_seq = 1
    and pat_pop_sequence.pat_population_review_dt <= last_day(current_date)
