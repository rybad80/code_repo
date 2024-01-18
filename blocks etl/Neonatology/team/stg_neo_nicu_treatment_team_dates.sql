select
    visit_key,
    provider_care_team_group_name as treatment_team,
    provider_care_team_start_date as start_date,
    provider_care_team_end_date as end_date,
    row_number() over (
        partition by visit_key
        order by provider_care_team_start_date asc
    ) as treatment_team_rn
from
    {{ref('provider_encounter_care_team')}}
where
    provider_care_team_group_category = 'neonatology'
