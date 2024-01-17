with all_nicu_ecmo_runs as (
    select
        flowsheet_ecmo.visit_key + cast(extract(epoch from flowsheet_ecmo.ecmo_end_datetime) as int)
        as ecmo_run_key,
        round(flowsheet_ecmo.ecmo_run_time_hours / 24.0, 1) as ecmo_run_time_days,
        flowsheet_ecmo.cannulation_type,
        date_trunc('day', flowsheet_ecmo.ecmo_end_datetime) as ecmo_end_day,
        coalesce(neo_nicu_episode.episode_end_date, current_date) as nicu_episode_end_or_current_date,
        neo_nicu_treatment_team.treatment_team,
        row_number() over (
            partition by ecmo_run_key
            order by neo_nicu_episode.episode_start_date
        ) as nicu_episode_order,
        row_number() over (
            partition by ecmo_run_key
            order by neo_nicu_treatment_team.treatment_team_start_date
        ) as treatment_team_order
    from
        {{ ref('flowsheet_ecmo') }} as flowsheet_ecmo
        inner join {{ ref('neo_nicu_episode') }} as neo_nicu_episode
            on neo_nicu_episode.visit_key = flowsheet_ecmo.visit_key
        left join {{ ref('neo_nicu_treatment_team') }} as neo_nicu_treatment_team
            on neo_nicu_treatment_team.visit_key = flowsheet_ecmo.visit_key
            and flowsheet_ecmo.ecmo_start_datetime between
                neo_nicu_treatment_team.treatment_team_start_date
                    and coalesce(neo_nicu_treatment_team.treatment_team_end_date, current_date)

    where
        /* patient was on ecmo + in the NICU at some point */
        flowsheet_ecmo.ecmo_start_datetime between
        neo_nicu_episode.episode_start_date and nicu_episode_end_or_current_date
        or flowsheet_ecmo.ecmo_end_datetime between
        neo_nicu_episode.episode_start_date and nicu_episode_end_or_current_date
        or (
            flowsheet_ecmo.ecmo_start_datetime < neo_nicu_episode.episode_start_date
            and flowsheet_ecmo.ecmo_end_datetime > nicu_episode_end_or_current_date
        )
)

select
    ecmo_run_key,
    ecmo_end_day,
    ecmo_run_time_days,
    cannulation_type,
    coalesce(treatment_team, 'No NICU Treatment Team') as treatment_team_at_ecmo_start
from
    all_nicu_ecmo_runs
where
    treatment_team_order = 1
    and nicu_episode_order = 1
