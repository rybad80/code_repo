with encounter_details as (
-- region accounting for null discharge dates due to non-inpatient encounter AND patient not yet discharged
    select
        stg_encounter.patient_key,
        stg_encounter.encounter_key,
        stg_encounter.csn,
        stg_encounter.mrn,
        stg_encounter.encounter_type,
        coalesce(stg_encounter.hospital_admit_date, stg_encounter.encounter_date) as encounter_start_date,
        case when
            stg_encounter.hospital_admit_date is null then stg_encounter.hospital_discharge_date
            else coalesce(stg_encounter.hospital_discharge_date, current_date)
        end as encounter_end_or_current_date
    from
        {{ ref('stg_encounter') }} as stg_encounter
-- end region
)

select
    bioresponse_infectious_episodes.patient_key,
    encounter_details.encounter_key,
    encounter_details.mrn,
    encounter_details.csn,
    bioresponse_infectious_episodes.diagnosis_hierarchy_1,
    bioresponse_infectious_episodes.positive_lab_ind,
    bioresponse_infectious_episodes.problem_list_ind,
    bioresponse_infectious_episodes.isolation_ind,
    bioresponse_infectious_episodes.episode_start_date,
    bioresponse_infectious_episodes.episode_end_date,
    encounter_details.encounter_type
from
    {{ ref('bioresponse_infectious_episodes') }} as bioresponse_infectious_episodes
    inner join encounter_details
        on bioresponse_infectious_episodes.patient_key = encounter_details.patient_key --noqa
        and bioresponse_infectious_episodes.episode_start_date <= encounter_details.encounter_end_or_current_date
        and bioresponse_infectious_episodes.episode_end_date >= encounter_details.encounter_start_date
