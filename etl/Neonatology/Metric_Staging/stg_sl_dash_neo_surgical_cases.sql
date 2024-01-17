select
    surgery_encounter.or_key,
    surgery_encounter.service,
    case
        when lower(surgery_encounter.location_group) != 'main or' then 'Bedside' else 'OR'
    end as surgery_location_group,
    date_trunc('day', surgery_encounter.surgery_date) as surgery_day
from
    {{ ref('surgery_encounter') }} as surgery_encounter
    inner join {{ ref('stg_sl_dash_neo_episodes') }} as stg_sl_dash_neo_episodes
        on stg_sl_dash_neo_episodes.visit_key = surgery_encounter.visit_key
            and surgery_encounter.surgery_date between
            date(stg_sl_dash_neo_episodes.episode_start_date)
            and coalesce(
                date(stg_sl_dash_neo_episodes.episode_end_date),
                current_date
            )
