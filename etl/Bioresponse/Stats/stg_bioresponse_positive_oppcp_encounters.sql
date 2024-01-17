with positive_oppcp as ( -- noqa: PRS
    select
        bioresponse_encounters_with_positives.diagnosis_hierarchy_1,
        stg_encounter_outpatient.patient_key,
        stg_encounter_outpatient.encounter_key,
        stg_encounter_outpatient.encounter_date,
        stg_encounter_outpatient.department_name,
        1 as positive_ind
    from
        {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
        inner join {{ ref('bioresponse_encounters_with_positives') }} as bioresponse_encounters_with_positives
            on stg_encounter_outpatient.encounter_key = bioresponse_encounters_with_positives.encounter_key
    where
        stg_encounter_outpatient.encounter_date >= {{ var('start_data_date') }}
        and stg_encounter_outpatient.primary_care_ind = 1
        and stg_encounter_outpatient.appointment_status = 'COMPLETED'
)

select
    positive_oppcp.diagnosis_hierarchy_1,
    positive_oppcp.encounter_date as stat_date,
    sum(positive_oppcp.positive_ind) as stat_numerator_val
from
    positive_oppcp
group by
    positive_oppcp.diagnosis_hierarchy_1,
    positive_oppcp.encounter_date
