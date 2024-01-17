-- TODO incude negative pressure rooms, which is different from siu and pstu
select
    stg_encounter_inpatient.patient_key,
    stg_encounter_inpatient.encounter_key,
    provider_encounter_care_team.provider_care_team_start_date as enter_date,
    provider_encounter_care_team.provider_care_team_end_date as exit_date,
    provider_encounter_care_team.provider_care_team_group_name as unit_name
from
     {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
    inner join {{ ref('provider_encounter_care_team') }} as provider_encounter_care_team
        on provider_encounter_care_team.encounter_key = stg_encounter_inpatient.encounter_key
where
    -- special isolation unit and patient special treatment
    provider_encounter_care_team.provider_care_team_group_name in ('siu', 'pstu')
    and (
        (
            provider_encounter_care_team.source_summary = 'provider_record_ser'
            -- start of COVID, when these rooms were sest up
            and provider_encounter_care_team.provider_care_team_start_date >= '2020-03-15'
        ) or provider_encounter_care_team.source_summary = 'provider_care_team_pct'
    )
