/* reports out consecutive calendar days of med exposure by `med_category` */
with
med_days as (
    /* Need to calculate the duration of therapy for the aminoglycosides
    (amikacin, gentamicin, tobramycin, and vancomycin) individually.
    And we need to combine them with the other ntmx meds to calculate the total duration
    of exposure. */

    /* Here each aminoglycoside med gets its own category */
    select
        visit_key,
        ntmx_grouper as med_category,
        action_date as med_date
    from
        {{ ref('stg_naki_meds') }}
    where
        ntmx_grouper in ('amikacin', 'gentamicin', 'tobramycin', 'vancomycin')

    union all

    /* Here we pull in the rest of the ntmx meds */
    select
        visit_key,
        'any_ntmx_med' as med_category,
        action_date as med_date
    from
        {{ ref('stg_naki_med_count') }}
    where
        ntmx_med_count > 0
),
prior_and_next_med as (
    select
        visit_key,
        med_date,
        med_category,
        lag(med_date) over (
            partition by
                visit_key,
                med_category
            order by med_date
        ) as prior_med_date,
        lead(med_date) over (
            partition by
                visit_key,
                med_category
            order by med_date
        ) as  next_med_date,
        prior_med_date - med_date as prior_diff,
        next_med_date - med_date as next_diff
    from
        med_days
),
med_starts as (
    select
        visit_key,
        med_category,
        med_date
    from
        prior_and_next_med
    where
        prior_diff != -1
        or prior_diff is null
),
med_stops as (
    select
        visit_key,
        med_category,
        med_date
    from
        prior_and_next_med
    where
        next_diff != 1
        or next_diff is null
)
select
    med_starts.visit_key,
    med_starts.med_category,
    med_starts.med_date as med_streak_start_date,
    min(med_stops.med_date) as med_streak_end_date
from
    med_starts
    inner join med_stops
        on med_stops.visit_key = med_starts.visit_key
            and med_stops.med_date >= med_starts.med_date
            and med_stops.med_category = med_starts.med_category
group by
    med_starts.visit_key,
    med_starts.med_category,
    med_starts.med_date
