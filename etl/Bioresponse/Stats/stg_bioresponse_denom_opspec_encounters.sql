with positive_oppcp as ( -- noqa: PRS
    select
        stg_encounter_outpatient.encounter_date as stat_date
    from
        {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
    where
        stg_encounter_outpatient.encounter_date >= {{ var('start_data_date') }}
        and stg_encounter_outpatient.specialty_care_ind = 1
        and stg_encounter_outpatient.appointment_status = 'COMPLETED'
)

select
    positive_oppcp.stat_date,
    sum(1) as stat_denominator_val
from
    positive_oppcp
group by
    positive_oppcp.stat_date
