/* Query pulling reporting pieces specific to the patient's encounter
and patient status at the time of the encounter
Final granularity should be one row per infection*/

with encounter_diagnoses as (
-- identifying if patient, in recent encounter, had a smoker status or is/was pregnant
-- NOTE: this data field is really only accurate when reporting recent cases of pts with regular visits
    select
        diagnosis_encounter_all.patient_key,
        max(
            case when
                diagnosis_encounter_all.icd10_code in ('F17.200', 'F17.210', 'Z72.0')
                then 1
                else 0
                end
        ) as current_smoker_ind,
        max(
            case when diagnosis_encounter_all.icd10_code = 'Z87.891'
            then 1
            else 0
            end
        ) as former_smoker_ind,
        max(
            case when
                diagnosis_encounter_all.icd10_code = 'Z34.90'
                and diagnosis_encounter_all.encounter_date >= current_date - interval('1 month')
            then 1
            else 0
            end
    ) as pregnant_ind
    from
        {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    where
        diagnosis_encounter_all.icd10_code in (
            'F17.200', 'F17.210', 'Z72.0', -- current smoker
            'Z87.891', -- former smoker
            'Z34.90' -- pregnant
        )
        and diagnosis_encounter_all.visit_diagnosis_ind = 1
        and diagnosis_encounter_all.encounter_date >= current_date - interval('3 months')
    group by
        diagnosis_encounter_all.patient_key
)

select
    stg_encounter.csn,
    stg_encounter.encounter_key,
    encounter_inpatient.primary_dx_icd,
    encounter_inpatient.primary_dx,
    encounter_diagnoses.current_smoker_ind,
    encounter_diagnoses.former_smoker_ind,
    encounter_diagnoses.pregnant_ind,
    stg_encounter.hospital_discharge_date as discharge_date,
    case when
        stg_encounter.age_days <= 0 then '0 d'
        when stg_encounter.age_days <= 31 then stg_encounter.age_days::integer ||  ' d'
        when stg_encounter.age_years < 2 then stg_encounter.age_months::integer || ' m'
        else stg_encounter.age_years::integer || ' y'
    end as patient_age
from
    {{ ref('stg_encounter') }} as stg_encounter
    left join encounter_diagnoses
        on stg_encounter.patient_key = encounter_diagnoses.patient_key
    left join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on stg_encounter.encounter_key = encounter_inpatient.encounter_key
