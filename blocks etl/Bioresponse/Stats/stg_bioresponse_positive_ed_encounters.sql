with positive_ed as ( -- noqa: PRS
    select
        bioresponse_encounters_with_positives.diagnosis_hierarchy_1,
        stg_encounter_ed.patient_key,
        stg_encounter_ed.encounter_key,
        stg_encounter_ed.encounter_date,
        stg_encounter_ed.department_name,
        case
            when stg_encounter_ed.initial_ed_department_center_abbr = 'CHOP MAIN' then 'PHL'
            else 'KOP'
        end as campus,
        stg_encounter_ed.inpatient_ind,
        1 as positive_ind
    from
        {{ ref('stg_encounter_ed') }} as stg_encounter_ed
        inner join {{ ref('bioresponse_encounters_with_positives') }} as bioresponse_encounters_with_positives
            on stg_encounter_ed.encounter_key = bioresponse_encounters_with_positives.encounter_key
    where
        stg_encounter_ed.encounter_date >= {{ var('start_data_date') }}
)

select
    positive_ed.campus,
    positive_ed.diagnosis_hierarchy_1,
    positive_ed.encounter_date as stat_date,
    sum(positive_ed.positive_ind) as stat_numerator_val
from
    positive_ed
group by
    positive_ed.campus,
    positive_ed.diagnosis_hierarchy_1,
    positive_ed.encounter_date
