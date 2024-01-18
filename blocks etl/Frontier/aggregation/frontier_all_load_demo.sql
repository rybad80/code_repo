with
age_base as (--region
    select
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date,
        metric_level_fy,
        'Age' as metric_name,
        case
            when stg_patient.current_age is null then 'No Age'
            else lookup_demographic_groups.category end as metric_level,
        count(distinct frontier_all_build_demo.pat_key) as metric_value
    from
        {{ ref('frontier_all_build_demo') }} as frontier_all_build_demo
        inner join {{ ref('stg_patient') }} as stg_patient
            on frontier_all_build_demo.mrn = stg_patient.mrn
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on frontier_all_build_demo.visit_key = stg_encounter.visit_key
        left join {{ ref('lookup_demographic_groups') }} as lookup_demographic_groups
            on stg_patient.current_age
            between lookup_demographic_groups.min_age and lookup_demographic_groups.max_age
    group by
        metric_level,
        metric_level_fy,
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date
    --end region
),
sex_base as (--region
    select
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date,
        metric_level_fy,
        'Sex' as metric_name,
        case when stg_patient.sex is null then 'Not Recorded'
        else lookup_demographic_groups.category end as metric_level,
        count(distinct frontier_all_build_demo.pat_key) as metric_value
    from
        {{ ref('frontier_all_build_demo') }} as frontier_all_build_demo
        inner join {{ ref('stg_patient') }} as stg_patient
            on frontier_all_build_demo.mrn = stg_patient.mrn
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on frontier_all_build_demo.visit_key = stg_encounter.visit_key
        left join {{ ref('lookup_demographic_groups') }} as lookup_demographic_groups
            on stg_patient.sex =  lookup_demographic_groups.source_sex
    group by
        metric_level,
        metric_level_fy,
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date
    --end region
),
race_base as (--region
    select
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date,
        metric_level_fy,
        'Race' as metric_name,
        stg_patient.race as metric_level,
        count(distinct frontier_all_build_demo.pat_key) as metric_value
    from
        {{ ref('frontier_all_build_demo') }} as frontier_all_build_demo
        inner join {{ ref('stg_patient') }} as stg_patient
            on frontier_all_build_demo.mrn = stg_patient.mrn
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on frontier_all_build_demo.visit_key = stg_encounter.visit_key
    where
        metric_level is not null
    group by
        metric_level,
        metric_level_fy,
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date
    --end region
),
race_ethnicity_base as (--region
    select
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date,
        metric_level_fy,
        'Race Ethnicity' as metric_name,
        stg_patient.race_ethnicity as metric_level,
        count(distinct frontier_all_build_demo.pat_key) as metric_value
    from
        {{ ref('frontier_all_build_demo') }} as frontier_all_build_demo
        inner join {{ ref('stg_patient') }} as stg_patient
            on frontier_all_build_demo.mrn = stg_patient.mrn
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on frontier_all_build_demo.visit_key = stg_encounter.visit_key
    where
        metric_level is not null
    group by
        metric_level,
        metric_level_fy,
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date
    --end region
),
ethnicity_base as (--region
    select
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date,
        metric_level_fy,
        'Ethnicity' as metric_name,
        stg_patient.ethnicity as metric_level,
        count(distinct frontier_all_build_demo.pat_key) as metric_value
    from
        {{ ref('frontier_all_build_demo') }} as frontier_all_build_demo
        inner join {{ ref('stg_patient') }} as stg_patient
            on frontier_all_build_demo.mrn = stg_patient.mrn
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on frontier_all_build_demo.visit_key = stg_encounter.visit_key
    where
        metric_level is not null
    group by
        metric_level,
        metric_level_fy,
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date
    --end region
),
language_base as (--region
    select
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date,
        metric_level_fy,
        'Preferred Language' as metric_name,
        stg_patient.preferred_language as metric_level,
        count(distinct frontier_all_build_demo.pat_key) as metric_value
    from
        {{ ref('frontier_all_build_demo') }} as frontier_all_build_demo
        inner join {{ ref('stg_patient') }} as stg_patient
            on frontier_all_build_demo.mrn = stg_patient.mrn
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on frontier_all_build_demo.visit_key = stg_encounter.visit_key
    where
        metric_level is not null
    group by
        metric_level,
        metric_level_fy,
        program_name,
        sub_cohort,
        frontier_all_build_demo.visit_key,
        frontier_all_build_demo.mrn,
        encounter_date
    --end region
),
final_union as (--region
        select
            program_name,
            metric_level_fy,
            sub_cohort,
            metric_name,
            metric_level,
            metric_value,
            mrn,
            visit_key,
            encounter_date
        from age_base
        union all
        select
            program_name,
            metric_level_fy,
            sub_cohort,
            metric_name,
            metric_level,
            metric_value,
            mrn,
            visit_key,
            encounter_date
        from sex_base
        union all
        select
            program_name,
            metric_level_fy,
            sub_cohort,
            metric_name,
            metric_level,
            metric_value,
            mrn,
            visit_key,
            encounter_date
        from race_base
        union all
        select
            program_name,
            metric_level_fy,
            sub_cohort,
            metric_name,
            metric_level,
            metric_value,
            mrn,
            visit_key,
            encounter_date
        from race_ethnicity_base
        union all
        select
            program_name,
            metric_level_fy,
            sub_cohort,
            metric_name,
            metric_level,
            metric_value,
            mrn,
            visit_key,
            encounter_date
        from ethnicity_base
        union all
        select
            program_name,
            metric_level_fy,
            sub_cohort,
            metric_name,
            metric_level,
            metric_value,
            mrn,
            visit_key,
            encounter_date
        from language_base
    --end region
)
select distinct
    row_number() over(order by program_name)
    as primary_key,
    visit_key,
    mrn,
    encounter_date,
    program_name,
    metric_level_fy,
    sub_cohort,
    metric_name,
    metric_level,
    metric_value
from final_union
where metric_level_fy is not null
