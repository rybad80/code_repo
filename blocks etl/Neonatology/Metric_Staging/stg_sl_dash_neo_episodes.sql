with cohort_groups as (
    select
        pat_key,
        cohort_group_display_name
    from
        {{ ref('neo_nicu_cohort_group') }}
    where
        cohort_group_name in (
            'cdh',
            'hie',
            'intestinal atresia',
            'lung lesion',
            'bpd',
            'open neural tube',
            'lymphatics'
            )
)

select
    neo_nicu_episode.episode_key,
    neo_nicu_episode.visit_key,
    neo_nicu_episode.pat_key,
    neo_nicu_episode.episode_start_date,
    neo_nicu_episode.episode_end_date,
    date_trunc('day', neo_nicu_episode.episode_end_date) as episode_end_day,
    date_trunc('day', neo_nicu_episode.hospital_discharge_date) as hospital_discharge_day,
    neo_nicu_episode.hospital_admit_date,
    neo_nicu_episode.hospital_discharge_date,
    neo_nicu_episode.nicu_los_days,
    neo_nicu_episode.hospital_los_days,
    case
        when cohort_groups.pat_key is null then 'Other'
        else cohort_groups.cohort_group_display_name
    end as cohort_group,
    /* this should really just be in our episode stack */
    case
        when lower(adt_department_group.department_group_name) in (
            'nicu',
            'sdu',
            'ed'
        ) then adt_department_group.department_group_name
        else 'Other'
    end as admission_source,
    case
        when neo_nicu_episode.hospital_discharge_date is not null then 1 else 0
    end as hospital_discharged_ind,
    1 as nicu_discharged_ind
from
    {{ ref('neo_nicu_episode')}} as neo_nicu_episode
    left join cohort_groups
        on cohort_groups.pat_key = neo_nicu_episode.pat_key
    inner join {{ ref('adt_department_group')}} as adt_department_group
        on adt_department_group.visit_key = neo_nicu_episode.visit_key
            and adt_department_group.enter_date = neo_nicu_episode.hospital_admit_date
