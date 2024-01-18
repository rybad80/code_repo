with final_nicu_ts as (
    select
        pat_key,
        max(coalesce(episode_end_date, current_date)) as final_nicu_ts
    from
        {{ ref('neo_nicu_episode') }}
    group by
        pat_key
)

select
    neo_nicu_episode.pat_key,
    min(procedure_order_clinical.placed_date) as cohort_group_enter_date
from
    {{ ref('neo_nicu_episode') }} as neo_nicu_episode
    inner join final_nicu_ts
        on final_nicu_ts.pat_key = neo_nicu_episode.pat_key
    inner join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_clinical.visit_key = neo_nicu_episode.visit_key
where
    /* consult to lymphatics program */
    procedure_order_clinical.procedure_id = 105185
    and procedure_order_clinical.placed_date < final_nicu_ts.final_nicu_ts
group by
    neo_nicu_episode.pat_key
