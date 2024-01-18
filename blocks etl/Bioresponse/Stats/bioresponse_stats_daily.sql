with enum_dx as ( -- noqa: PRS,L01
    select distinct
        lookup_bioresponse_diagnosis.diagnosis_hierarchy_1
    from
        {{ ref('lookup_bioresponse_diagnosis') }} as lookup_bioresponse_diagnosis
),

enum_loc as (
    select
        stg_bioresponse_denom_ed_encounters.campus
    from
        {{ ref('stg_bioresponse_denom_ed_encounters') }} as stg_bioresponse_denom_ed_encounters
    union
    select
        stg_bioresponse_denom_ip_days.campus
    from
        {{ ref('stg_bioresponse_denom_ip_days') }} as stg_bioresponse_denom_ip_days
),

enum_date as (
    select
        dim_date.full_date
    from
        {{ ref('dim_date') }} as dim_date
    where
        dim_date.full_date >= {{ var('start_data_date') }}
        and dim_date.full_date <= current_date
),

spine_w_campus as (
    select
        enum_date.full_date as stat_date,
        enum_loc.campus,
        enum_dx.diagnosis_hierarchy_1
    from
        enum_date
        cross join enum_loc
        cross join enum_dx
),

spine_wout_campus as (
    select
        enum_date.full_date as stat_date,
        enum_dx.diagnosis_hierarchy_1
    from
        enum_date
        cross join enum_dx
),

stat_positive_ed as (
    select
        spine.campus,
        spine.diagnosis_hierarchy_1,
        spine.stat_date,
        '% Positive ED Encounters' as stat_name,
        coalesce(stg_bioresponse_positive_ed_encounters.stat_numerator_val, 0) as stat_numerator_val,
        coalesce(stg_bioresponse_denom_ed_encounters.stat_denominator_val, 0) as stat_denominator_val
    from
        spine_w_campus as spine
        left join {{ ref('stg_bioresponse_positive_ed_encounters') }} as stg_bioresponse_positive_ed_encounters
            on spine.stat_date = stg_bioresponse_positive_ed_encounters.stat_date
            and spine.campus = stg_bioresponse_positive_ed_encounters.campus
            and spine.diagnosis_hierarchy_1 = stg_bioresponse_positive_ed_encounters.diagnosis_hierarchy_1
        left join {{ ref('stg_bioresponse_denom_ed_encounters') }} as stg_bioresponse_denom_ed_encounters
            on spine.stat_date = stg_bioresponse_denom_ed_encounters.stat_date
            and spine.campus = stg_bioresponse_denom_ed_encounters.campus
),

stat_positive_ip as (
    select
        spine.campus,
        spine.diagnosis_hierarchy_1,
        spine.stat_date,
        '% Positive IP Days' as stat_name,
        coalesce(stg_bioresponse_positive_ip_days.stat_numerator_val, 0) as stat_numerator_val,
        coalesce(stg_bioresponse_denom_ip_days.stat_denominator_val, 0) as stat_denominator_val
    from
        spine_w_campus as spine
        left join {{ ref('stg_bioresponse_positive_ip_days') }} as stg_bioresponse_positive_ip_days
            on spine.stat_date = stg_bioresponse_positive_ip_days.stat_date
            and spine.campus = stg_bioresponse_positive_ip_days.campus
            and spine.diagnosis_hierarchy_1 = stg_bioresponse_positive_ip_days.diagnosis_hierarchy_1
        left join {{ ref('stg_bioresponse_denom_ip_days') }} as stg_bioresponse_denom_ip_days
            on spine.stat_date = stg_bioresponse_denom_ip_days.stat_date
            and spine.campus = stg_bioresponse_denom_ip_days.campus
),

stat_positive_labs as (
    select
        'All' as campus,
        spine.diagnosis_hierarchy_1,
        spine.stat_date,
        '% Positive Labs' as stat_name,
        coalesce(stg_bioresponse_positive_labs.stat_numerator_val, 0) as stat_numerator_val,
        coalesce(stg_bioresponse_denom_lab_tests.stat_denominator_val, 0) as stat_denominator_val
    from
        spine_wout_campus as spine
        left join {{ ref('stg_bioresponse_positive_labs') }} as stg_bioresponse_positive_labs
            on spine.stat_date = stg_bioresponse_positive_labs.stat_date
            and spine.diagnosis_hierarchy_1 = stg_bioresponse_positive_labs.diagnosis_hierarchy_1 -- noqa
        left join {{ ref('stg_bioresponse_denom_lab_tests') }} as stg_bioresponse_denom_lab_tests -- noqa
            on spine.stat_date = stg_bioresponse_denom_lab_tests.stat_date
            and spine.diagnosis_hierarchy_1 = stg_bioresponse_denom_lab_tests.diagnosis_hierarchy_1
),

stat_positive_oppcp as (
    select
        'All' as campus,
        spine.diagnosis_hierarchy_1,
        spine.stat_date,
        '% Positive OP Primary Care Encounters' as stat_name,
        coalesce(stg_bioresponse_positive_oppcp_encounters.stat_numerator_val, 0) as stat_numerator_val,
        coalesce(stg_bioresponse_denom_opspec_encounters.stat_denominator_val, 0) as stat_denominator_val
    from
        spine_wout_campus as spine
        left join {{ ref('stg_bioresponse_positive_oppcp_encounters') }}
            as stg_bioresponse_positive_oppcp_encounters
            on spine.stat_date = stg_bioresponse_positive_oppcp_encounters.stat_date
            and spine.diagnosis_hierarchy_1 = stg_bioresponse_positive_oppcp_encounters.diagnosis_hierarchy_1
        left join {{ ref('stg_bioresponse_denom_opspec_encounters') }} as stg_bioresponse_denom_opspec_encounters
            on spine.stat_date = stg_bioresponse_denom_opspec_encounters.stat_date
),

stat_positive_opspec as (
    select
        'All' as campus,
        spine.diagnosis_hierarchy_1,
        spine.stat_date,
        '% Positive OP Specialty Care Encounters' as stat_name,
        coalesce(stg_bioresponse_positive_opspec_encounters.stat_numerator_val, 0) as stat_numerator_val,
        coalesce(stg_bioresponse_denom_opspec_encounters.stat_denominator_val, 0) as stat_denominator_val
    from
        spine_wout_campus as spine
        left join {{ ref('stg_bioresponse_positive_opspec_encounters') }}
            as stg_bioresponse_positive_opspec_encounters
            on spine.stat_date = stg_bioresponse_positive_opspec_encounters.stat_date
            and spine.diagnosis_hierarchy_1 = stg_bioresponse_positive_opspec_encounters.diagnosis_hierarchy_1
        left join {{ ref('stg_bioresponse_denom_opspec_encounters') }} as stg_bioresponse_denom_opspec_encounters
            on spine.stat_date = stg_bioresponse_denom_opspec_encounters.stat_date
),

stat_all as (
    select * from stat_positive_ed
    union all
    select * from stat_positive_ip
    union all
    select * from stat_positive_labs
    union all
    select * from stat_positive_oppcp
    union all
    select * from stat_positive_opspec
)

select
    campus,
    diagnosis_hierarchy_1,
    stat_name,
    stat_date,
    stat_numerator_val,
    stat_denominator_val,
    case
        when stat_denominator_val is null then 0
        when stat_denominator_val = 0 then 0
        else stat_numerator_val::float / stat_denominator_val::float
    end as stat_rate
from
    stat_all
where
    campus in ('PHL', 'All')
    or (
        campus = 'KOP'
        and stat_date >= '2022-01-01'
    )
