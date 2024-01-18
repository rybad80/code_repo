/* Table to pull the patients that have positive lab tests that require reporting to DOH
Granularity is one row per lab-test in each inpatient infectious episode
limiting patients to those with an episode in the last year, 
covering more then the regular reporting needs */

select
    bioresponse_encounters_with_positives.patient_key,
    bioresponse_encounters_with_positives.encounter_key,
    {{
        dbt_utils.surrogate_key([
            'bioresponse_encounters_with_positives.encounter_key',
            'bioresponse_encounters_with_positives.diagnosis_hierarchy_1',
            'bioresponse_lab_results.diagnosis_hierarchy_2',
            'bioresponse_encounters_with_positives.episode_start_date',
        ])
    }} as encounter_episode_key,
    bioresponse_lab_results.csn,
    stg_encounter_inpatient.ip_enter_date,
    coalesce(stg_encounter_inpatient.icu_ind, 0) as icu_ind,
    bioresponse_encounters_with_positives.diagnosis_hierarchy_1,
    bioresponse_lab_results.diagnosis_hierarchy_2,
    bioresponse_lab_results.placed_date,
    bioresponse_lab_results.specimen_taken_date,
    bioresponse_lab_results.result_date,
    bioresponse_encounters_with_positives.episode_start_date,
    bioresponse_lab_results.procedure_name,
    bioresponse_lab_results.result_component_name,
    bioresponse_lab_results.order_specimen_source,
    bioresponse_lab_results.placed_date + interval('30 days') as thirty_day_window,
    row_number() over (
        partition by
            bioresponse_encounters_with_positives.encounter_key,
            bioresponse_encounters_with_positives.diagnosis_hierarchy_1,
            bioresponse_lab_results.diagnosis_hierarchy_2
        order by
            result_date
    ) as order_of_tests
from
    {{ ref('bioresponse_encounters_with_positives') }} as bioresponse_encounters_with_positives
    inner join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
        on bioresponse_encounters_with_positives.encounter_key = stg_encounter_inpatient.encounter_key
    inner join {{ ref('bioresponse_lab_results') }} as bioresponse_lab_results
        on bioresponse_lab_results.diagnosis_hierarchy_1 = bioresponse_encounters_with_positives.diagnosis_hierarchy_1 --noqa
        and bioresponse_lab_results.placed_date >= bioresponse_encounters_with_positives.episode_start_date
        and bioresponse_lab_results.placed_date <= bioresponse_encounters_with_positives.episode_end_date
where
    bioresponse_encounters_with_positives.positive_lab_ind = 1
    and bioresponse_encounters_with_positives.episode_start_date >= current_date - interval('1 year')
    and bioresponse_lab_results.positive_ind = 1
