with tiae_cohort as (
    select
        stg_nears_redcap.encounter_month,
        stg_nears_redcap.encounter_id,
        stg_nears_redcap.center_name,
        stg_nears_redcap.location_of_intubation,
        stg_nears_redcap.indication_type,
        stg_nears_redcap.course_id,
        stg_nears_redcap.tiaes_course_ind,
        stg_nears_redcap.num_of_attempts,
        stg_nears_redcap.pulse_lowest,
        stg_nears_redcap.pulse_start
    from
        {{ ref('stg_nears_redcap') }} as stg_nears_redcap
    where
        stg_nears_redcap.registry = 'NEAR4KIDS'
        and stg_nears_redcap.location_of_intubation in ('CICU', 'PICU')
)

select
    {{
    dbt_utils.surrogate_key([
        'center_name',
        'location_of_intubation',
        'encounter_month'
        ])
    }} as location_month_key,
    center_name,
    location_of_intubation,
    encounter_month,
    count(
        case
            when tiaes_course_ind = 1 then 1
        end
    ) as tiaes_count,
    count(distinct encounter_id) as total_intubations,
    count(course_id) as total_courses,
    count(distinct
        case
            when indication_type = 'Primary Intubation' then encounter_id
    end) as primary_intubation_count,
    count(distinct
        case
            when indication_type = 'Change of Tube' then encounter_id
    end) as change_of_tube_count,
    count(
        case
            when num_of_attempts > 2
            then 1
    end) as multiple_attempts_count,
    count(
        case
            when pulse_lowest < 80 and pulse_start > 90
            then 1
    end) as desat_80_count,
    count(
        case
            when pulse_lowest < 90 and pulse_start > 90
            then 1
    end) as desat_90_count
from
    tiae_cohort
group by
    center_name,
    location_of_intubation,
    encounter_month
