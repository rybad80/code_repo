with care_teams_raw as (
    --identify first and last care team of encounter
    --data starts prior to April 2021; cannot use stg_treatment_teams
    select
        asp_ip_cap_cohort.visit_key,
        case when asp_ip_cap_cohort.inpatient_admit_date
            < stg_provider_encounter_care_team.provider_care_team_start_date
            then 1 else 0 end as care_team_after_admit_ind,
        min(care_team_after_admit_ind) over (
            partition by asp_ip_cap_cohort.visit_key
        ) as all_care_teams_after_admit_ind,
        --Identify the LAST care team before inpatient admission (if there is one)
        --otherwise, identify the FIRST care team after inpatient admission
        case when all_care_teams_after_admit_ind  = 0
            then first_value(stg_provider_encounter_care_team.provider_care_team_name) over (
                partition by asp_ip_cap_cohort.visit_key
                order by
                    care_team_after_admit_ind,
                    stg_provider_encounter_care_team.provider_care_team_start_date desc,
                    stg_provider_encounter_care_team.provider_care_team_end_date
            )
            else first_value(stg_provider_encounter_care_team.provider_care_team_name) over (
                partition by asp_ip_cap_cohort.visit_key
                order by
                    care_team_after_admit_ind,
                    stg_provider_encounter_care_team.provider_care_team_start_date,
                    stg_provider_encounter_care_team.provider_care_team_end_date
            ) end as admission_team,
        --Identify the last care taem of the encounter
        first_value(stg_provider_encounter_care_team.provider_care_team_name) over (
            partition by asp_ip_cap_cohort.visit_key
            order by
                stg_provider_encounter_care_team.provider_care_team_start_date desc,
                stg_provider_encounter_care_team.provider_care_team_end_date desc
        ) as discharge_team
    from
        {{ ref('asp_ip_cap_cohort') }} as asp_ip_cap_cohort
        inner join {{ ref('stg_provider_encounter_care_team') }} as stg_provider_encounter_care_team
            on asp_ip_cap_cohort.visit_key = stg_provider_encounter_care_team.visit_key
    --Exclude ED care teams
    where stg_provider_encounter_care_team.provider_care_team_end_date
        > asp_ip_cap_cohort.inpatient_admit_date
)

select
    visit_key,
    admission_team,
    discharge_team
from
    care_teams_raw
group by
    visit_key,
    admission_team,
    discharge_team
