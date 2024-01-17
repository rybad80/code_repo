{% set aminoglycosides = ['amikacin', 'gentamicin', 'tobramycin', 'vancomycin'] %}

with
calendar_dates as (
    select
        full_dt as index_date
    from
        {{ source('cdw','master_date') }}
    where
        full_dt between date('2022-01-01') and current_date - 1
),
nicu_dates as (
    select
        neo_nicu_episode_phl.pat_key,
        neo_nicu_episode_phl.visit_key,
        neo_nicu_episode_phl.episode_key,
        neo_nicu_episode_phl.episode_start_date,
        neo_nicu_episode_phl.episode_end_date,
        calendar_dates.index_date,
        /* need row number in case there are two nicu episodes that overlap with the
        index date */
        row_number() over (
            partition by
                neo_nicu_episode_phl.visit_key,
                calendar_dates.index_date
            order by neo_nicu_episode_phl.episode_start_date desc
        ) as rn
    from
        {{ ref('neo_nicu_episode_phl') }} as neo_nicu_episode_phl
        inner join calendar_dates
            on calendar_dates.index_date between date(neo_nicu_episode_phl.episode_start_date)
                and coalesce(date(neo_nicu_episode_phl.episode_end_date), current_date)
),
daily_first_last_admin as (
    select
        visit_key,
        date(administration_date) as index_date,
        ntmx_grouper as med_category,
        min(administration_date) as first_admin_date,
        max(administration_date) as last_admin_date
    from
        {{ ref('stg_naki_med_admins') }}
    where
        ntmx_grouper in (
            {% for med in aminoglycosides %}
                '{{ med }}'{{ ',' if not loop.last }}
            {% endfor %}
        )
    group by
        visit_key,
        date(administration_date),
        ntmx_grouper
)
select
    nicu_dates.pat_key,
    nicu_dates.visit_key,
    nicu_dates.index_date,
    nicu_dates.episode_start_date,
    nicu_dates.episode_end_date,
    coalesce(stg_naki_med_count.ntmx_med_count, 0) as ntmx_meds_daily_count,
    coalesce(stg_naki_med_count.ntmx_med_names, 'n/a') as ntmx_meds_daily_names,
    any_ntmx_med_streak.med_streak_start_date as any_ntmx_med_start_date,
    any_ntmx_med_streak.med_streak_end_date as any_ntmx_med_end_date,
    coalesce(nicu_dates.index_date - any_ntmx_med_streak.med_streak_start_date + 1, 0)
        as any_ntmx_med_dot,
    {% for med in aminoglycosides %}
        {{ med }}_streak_start_date_admin.first_admin_date as {{ med }}_start,
        {{ med }}_index_date_admin.last_admin_date as {{ med }}_end,
        round(extract(epoch from {{ med }}_index_date_admin.last_admin_date
            - {{ med }}_streak_start_date_admin.first_admin_date
        ) / 3600.0, 2) as {{ med }}_duration_in_hours
        {{ ',' if not loop.last }}
    {% endfor %}
from
    nicu_dates
    left join {{ ref('stg_naki_med_count') }} as stg_naki_med_count
        on stg_naki_med_count.visit_key = nicu_dates.visit_key
        and stg_naki_med_count.action_date = nicu_dates.index_date
    left join {{ ref('stg_naki_med_streaks') }} as any_ntmx_med_streak
        on any_ntmx_med_streak.visit_key = nicu_dates.visit_key
        and any_ntmx_med_streak.med_streak_start_date <= nicu_dates.index_date
        and any_ntmx_med_streak.med_streak_end_date >= nicu_dates.index_date
        and any_ntmx_med_streak.med_category = 'any_ntmx_med'
    {% for med in aminoglycosides %}
        left join {{ ref('stg_naki_med_streaks') }} as {{ med }}_streak
            on {{ med }}_streak.visit_key = nicu_dates.visit_key
            and {{ med }}_streak.med_streak_start_date <= nicu_dates.index_date
            and {{ med }}_streak.med_streak_end_date >= nicu_dates.index_date
            and {{ med }}_streak.med_category = '{{ med }}'
        left join daily_first_last_admin as {{ med }}_streak_start_date_admin
            on {{ med }}_streak_start_date_admin.visit_key = nicu_dates.visit_key
            and {{ med }}_streak_start_date_admin.index_date
                = {{ med }}_streak.med_streak_start_date
            and {{ med }}_streak_start_date_admin.med_category = '{{ med }}'
        /* need latest med admin on INDEX_DATE, not final of day of streak.
        Important for seeing exposure history over time */
        left join daily_first_last_admin as {{ med }}_index_date_admin
            on {{ med }}_index_date_admin.visit_key = nicu_dates.visit_key
            and {{ med }}_index_date_admin.index_date = nicu_dates.index_date
            and {{ med }}_index_date_admin.med_category = '{{ med }}'
    {% endfor %}
where
    nicu_dates.rn = 1
