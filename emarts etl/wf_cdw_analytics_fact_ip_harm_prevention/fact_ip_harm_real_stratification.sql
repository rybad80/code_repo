-- get true line days
with denom_true as (
    select
        stg_harm_event_all_denominator.pat_key,
        stg_harm_event_all_denominator.dept_key,
        stg_harm_event_all_denominator.harm_event_dt,
        date_trunc('month', stg_harm_event_all_denominator.harm_event_dt) as harm_event_dt_month,
        stg_harm_event_all_denominator.harm_type,
        stg_harm_event_all_denominator.pat_race_ethnicity,
        stg_harm_event_all_denominator.pat_pref_lang,
        'denom_true' as stat_type,
        stg_harm_event_all_denominator.denominator_value as value
    from
        {{ ref('stg_harm_event_all_denominator') }} as stg_harm_event_all_denominator
),

-- get denominator counts associated with "FIX FOR HISTORICAL DISCREPANCIES"
denom_extra as (
    select
        stg_harm_event_all_post.pat_key,
        stg_harm_event_all_post.dept_key,
        stg_harm_event_all_post.harm_event_dt,
         date_trunc('month', stg_harm_event_all_post.harm_event_dt) as harm_event_dt_month,
        stg_harm_event_all_post.harm_type,
        'blank' as pat_race_ethnicity,
        'blank' as pat_pref_lang,
        'denom_extra' as stat_type,
        stg_harm_event_all_post.denominator_value as value
    from
        {{ ref('stg_harm_event_all_post') }} as stg_harm_event_all_post
),

-- get numerator events
num_events as (
    select
        stg_harm_event_all_numerator.pat_key,
        stg_harm_event_all_numerator.dept_key,
        stg_harm_event_all_numerator.event_dt,
         date_trunc('month', stg_harm_event_all_numerator.event_dt) as harm_event_dt_month,
        stg_harm_event_all_numerator.harm_type,
        stg_harm_event_all_numerator.pat_race_ethnicity,
        stg_harm_event_all_numerator.pat_pref_lang,
        'num_event_count' as stat_type,
        1 as value
    from
        {{ ref('stg_harm_event_all_numerator') }} as stg_harm_event_all_numerator
),

-- merge numerator and denominators
all_stats as (
    select * from denom_true
    union all
    select * from denom_extra
    union all
    select * from num_events
)

select
    pat_key,
    dept_key,
    harm_type,
    harm_event_dt,
    pat_race_ethnicity,
    pat_pref_lang,
    harm_event_dt_month,
    sum(
        case when stat_type = 'denom_true' then value else 0 end
    ) as denom_true,
    sum(
        case when stat_type = 'denom_extra' then value else 0 end
    ) as denom_extra,
    sum(
        case when stat_type = 'num_event_count' then value else 0 end
    ) as num_event_count
from
    all_stats
group by
    pat_key,
    dept_key,
    harm_type,
    harm_event_dt,
    pat_race_ethnicity,
    pat_pref_lang,
    harm_event_dt_month
