with ed_visits as ( -- noqa: PRS
    select
        stg_encounter_ed.encounter_date as stat_date,
        case
            when stg_encounter_ed.initial_ed_department_center_abbr = 'CHOP MAIN' then 'PHL'
            else 'KOP'
        end as campus
    from
        {{ ref('stg_encounter_ed') }} as stg_encounter_ed
    where
        stg_encounter_ed.encounter_date >= {{ var('start_data_date') }}
)

select
    ed_visits.stat_date,
    ed_visits.campus,
    sum(1) as stat_denominator_val
from
    ed_visits
group by
    ed_visits.stat_date,
    ed_visits.campus
