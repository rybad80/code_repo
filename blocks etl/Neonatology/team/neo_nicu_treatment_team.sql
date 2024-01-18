with encapsulated_treatment_team_dates as (
    /* CTE identifies treatment teams that are entirely encapsulated by another treatment team, so they can
    be dropped later. */
    select
        stg_neo_nicu_treatment_team_dates.visit_key,
        stg_neo_nicu_treatment_team_dates.treatment_team_rn
    from
        {{ ref('stg_neo_nicu_treatment_team_dates') }} as stg_neo_nicu_treatment_team_dates
        inner join {{ ref('stg_neo_nicu_treatment_team_dates') }} as encapsulating_treatment_team
            on encapsulating_treatment_team.visit_key = stg_neo_nicu_treatment_team_dates.visit_key
                and encapsulating_treatment_team.start_date <= stg_neo_nicu_treatment_team_dates.start_date
                and coalesce(encapsulating_treatment_team.end_date, current_timestamp)
                >= coalesce(stg_neo_nicu_treatment_team_dates.end_date, current_timestamp)
                and stg_neo_nicu_treatment_team_dates.treatment_team_rn
                != encapsulating_treatment_team.treatment_team_rn
),

treatment_team_dates as (
    /* This is our clean treatment team data. Treatment teams that were encapsulated are dropped here. */
    select
        stg_neo_nicu_treatment_team_dates.visit_key,
        stg_neo_nicu_treatment_team_dates.treatment_team,
        stg_neo_nicu_treatment_team_dates.start_date,
        stg_neo_nicu_treatment_team_dates.end_date
    from
        {{ ref('stg_neo_nicu_treatment_team_dates') }} as stg_neo_nicu_treatment_team_dates
        left join encapsulated_treatment_team_dates
            on encapsulated_treatment_team_dates.visit_key = stg_neo_nicu_treatment_team_dates.visit_key
                and encapsulated_treatment_team_dates.treatment_team_rn
                = stg_neo_nicu_treatment_team_dates.treatment_team_rn
    where
        encapsulated_treatment_team_dates.visit_key is null
),

first_team_start_after_ep as (
    /* The first treatment team for an episode may have a `start_date` after the episode. Here we add a row
    number so that we can always find the first. */
    select
        neo_nicu_episode.visit_key,
        neo_nicu_episode.episode_start_date,
        treatment_team_dates.treatment_team,
        row_number() over (
            partition by
                neo_nicu_episode.visit_key,
                neo_nicu_episode.episode_start_date
            order by
                treatment_team_dates.start_date
        ) as treatment_team_order_asc

    from
        {{ ref('neo_nicu_episode') }} as neo_nicu_episode
        inner join treatment_team_dates
            on treatment_team_dates.visit_key = neo_nicu_episode.visit_key
                and treatment_team_dates.start_date >= neo_nicu_episode.episode_start_date
),

ep_start_active_team as (
    /* Captures the treatment team that was active at the instant of the episode start. */
    select
        neo_nicu_episode.visit_key,
        neo_nicu_episode.episode_start_date,
        treatment_team_dates.treatment_team,
        row_number() over (
            partition by
                neo_nicu_episode.visit_key,
                neo_nicu_episode.episode_start_date
            order by
                treatment_team_dates.start_date desc
        ) as treatment_team_order_desc
    from
        {{ ref('neo_nicu_episode') }} as neo_nicu_episode
        /* _active_ treatment team on episode start */
        left join treatment_team_dates
            on treatment_team_dates.visit_key = neo_nicu_episode.visit_key
                and treatment_team_dates.start_date <= neo_nicu_episode.episode_start_date
                and coalesce(treatment_team_dates.end_date, current_timestamp)
                >= neo_nicu_episode.episode_start_date
),

episode_first_treatment_team as (
    /* We want the first treatment team start date to always be the episode start -- but the data never exactly
    aligns.

    We will take the patient's first treatment team during their episode, and fix the start date of that to be
    the episode start.

    If a treatment team row is active when the episode starts, we use that (the `ep_start_active_team` CTE).

    When there is no active treatment team row, we use the first team during the episode
    (`first_team_start_after_ep`).
    */
    select
        neo_nicu_episode.visit_key,
        neo_nicu_episode.episode_start_date,
        neo_nicu_episode.episode_end_date,
        neo_nicu_episode.episode_start_date as start_date,
        coalesce(ep_start_active_team.treatment_team, first_team_start_after_ep.treatment_team) as treatment_team
    from
        {{ ref('neo_nicu_episode') }} as neo_nicu_episode
        left join ep_start_active_team
            on ep_start_active_team.visit_key = neo_nicu_episode.visit_key
                and ep_start_active_team.episode_start_date = neo_nicu_episode.episode_start_date
                and ep_start_active_team.treatment_team_order_desc = 1
        left join first_team_start_after_ep
            on first_team_start_after_ep.visit_key = neo_nicu_episode.visit_key
                and first_team_start_after_ep.episode_start_date = neo_nicu_episode.episode_start_date
                and first_team_start_after_ep.treatment_team_order_asc = 1
),

episode_treatment_team as (
    /* First clause in the union sql will provide just the first team row for each episode. */
    select
        episode_first_treatment_team.visit_key,
        episode_first_treatment_team.episode_start_date,
        episode_first_treatment_team.episode_end_date,
        episode_first_treatment_team.start_date,
        episode_first_treatment_team.treatment_team
    from
        episode_first_treatment_team

    union all

    /* Second clause in the union sql will provides the rest of the team rows. */
    select
        episode_first_treatment_team.visit_key,
        episode_first_treatment_team.episode_start_date,
        episode_first_treatment_team.episode_end_date,
        treatment_team_dates.start_date,
        treatment_team_dates.treatment_team
    from
        episode_first_treatment_team
        inner join treatment_team_dates
            on treatment_team_dates.visit_key = episode_first_treatment_team.visit_key
                and treatment_team_dates.start_date > episode_first_treatment_team.start_date
                and treatment_team_dates.start_date
                < coalesce(episode_first_treatment_team.episode_end_date, current_timestamp)
),

episode_treatment_team_w_prev as (
    /* Here we add the prior treatment team for each treatment team row. This allows us to identify cases where the
    same team is assigned back to back. Since we haven't set end dates yet, we can just drop the redundant rows.
    */
    select
        visit_key,
        episode_start_date,
        episode_end_date,
        start_date,
        treatment_team,
        lag(treatment_team) over (
            partition by
                visit_key,
                episode_start_date
            order by start_date
        ) as prev_treatment_team
    from
        episode_treatment_team
),

episode_treatment_team_final as (
    /* Finally, we calculate the end date for each treatment team row.
    If there is another treatment team during this episode, we use the next team start minus 1 minute.
    If it is the last treatment team, use the episode end (even if that is null). */
    select
        visit_key,
        episode_start_date,
        episode_end_date,
        start_date as treatment_team_start_date,
        coalesce(
            lead(start_date) over (
                partition by
                    visit_key,
                    episode_start_date
                order by start_date
            ) - interval '1 minute',
            episode_end_date
        ) as treatment_team_end_date,
        treatment_team,
        {{
           dbt_chop_utils.datetime_diff(
                from_date='treatment_team_start_date',
                to_date='treatment_team_end_date',
                unit='day'
            )
        }} as treatment_team_los_days,
        row_number() over (
            partition by
                visit_key,
                episode_start_date
            order by
                treatment_team_start_date
        ) as treatment_team_number
    from
        episode_treatment_team_w_prev
    where
        treatment_team != prev_treatment_team
        or prev_treatment_team is null
)

select
    {{
        dbt_utils.surrogate_key([
            'neo_nicu_episode.episode_key',
            'episode_treatment_team_final.treatment_team_number'
        ])
    }} as episode_treatment_team_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.sex,
    stg_patient.gestational_age_complete_weeks,
    stg_patient.gestational_age_remainder_days,
    floor(stg_patient.birth_weight_kg * 1000) as birth_weight_grams,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.hospital_discharge_date,
    neo_nicu_episode.episode_start_date,
    neo_nicu_episode.episode_end_date,
    episode_treatment_team_final.treatment_team,
    episode_treatment_team_final.treatment_team_start_date,
    episode_treatment_team_final.treatment_team_end_date,
    episode_treatment_team_final.treatment_team_los_days,
    episode_treatment_team_final.treatment_team_number,
    neo_nicu_episode.pat_key,
    neo_nicu_episode.visit_key,
    neo_nicu_episode.episode_key
from
    episode_treatment_team_final
    inner join {{ ref('neo_nicu_episode') }} as neo_nicu_episode
        on neo_nicu_episode.visit_key = episode_treatment_team_final.visit_key
            and neo_nicu_episode.episode_start_date = episode_treatment_team_final.episode_start_date
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on encounter_inpatient.visit_key = neo_nicu_episode.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = encounter_inpatient.pat_key
