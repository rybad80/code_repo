with pma_36 as (
    select
        pat_key,
        date(
            dob - gestational_age_remainder_days
            - gestational_age_complete_weeks * 7 + 36 * 7
        ) as pma_36_weeks_date,
        max(
            case
                when pma_36_weeks_date >= date(episode_start_date)
                    and pma_36_weeks_date <= date(coalesce(episode_end_date, current_date))
                    then 1
                else 0
            end
        ) as pma_36_in_nicu_ind
    from
        {{ ref('neo_nicu_episode') }}
    where
        gestational_age_complete_weeks < 32
    group by
        pat_key,
        pma_36_weeks_date
),
first_nicu_visit as (
    select
        pma_36.pat_key,
        min(neo_nicu_episode.episode_start_date)  as first_post_pma_36_nicu_start
    from
        pma_36
        left join {{ ref('neo_nicu_episode') }} as neo_nicu_episode
            on pma_36.pat_key = neo_nicu_episode.pat_key
            and pma_36.pma_36_in_nicu_ind = 0
    where
        date(neo_nicu_episode.episode_start_date) > pma_36.pma_36_weeks_date
    group by
        pma_36.pat_key
),
bpd_index as (
    select
        pma_36.pat_key,
        pma_36.pma_36_weeks_date,
        pma_36.pma_36_in_nicu_ind,
        case
            when pma_36.pma_36_in_nicu_ind = 1 then pma_36.pma_36_weeks_date
            else first_nicu_visit.first_post_pma_36_nicu_start
        end as bpd_index_date
    from
        pma_36
        left join first_nicu_visit
            on pma_36.pat_key = first_nicu_visit.pat_key
    group by
        pma_36.pat_key,
        pma_36.pma_36_weeks_date,
        pma_36.pma_36_in_nicu_ind,
        first_nicu_visit.first_post_pma_36_nicu_start
)

select distinct
    bpd_index.pat_key,
    bpd_index.bpd_index_date
from
    bpd_index
    inner join {{ ref('neo_nicu_respiratory_history') }} as neo_nicu_respiratory_history
        on bpd_index.pat_key = neo_nicu_respiratory_history.pat_key
where
    /* add 12 hours of buffer time to for that flowsheets data to start flowing for
    new nicu arrivals */
    neo_nicu_respiratory_history.resp_support_start_datetime <= bpd_index.bpd_index_date
    + cast('12 hours' as interval)
    /* do NOT include buffer when comparing stop time */
    and neo_nicu_respiratory_history.resp_support_stop_datetime
    >= bpd_index.bpd_index_date
