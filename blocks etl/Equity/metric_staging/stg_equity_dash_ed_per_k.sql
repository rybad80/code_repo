with ed_visit_lvl as (
    select
        stg_patient.pat_key,
        stg_encounter.visit_key,
        coalesce(stg_patient.race_ethnicity, 'Unknown') as race_ethnicity,
        'FY' || substring(dim_date.fiscal_year::varchar(5), 3, 2) as fiscal_year,
        case
            when stg_encounter_ed.visit_key is not null
            then 1 else 0
        end as numerator
    from
        {{ ref('stg_encounter') }} as stg_encounter
        inner join {{ ref('stg_patient') }} as stg_patient
            on stg_encounter.pat_key = stg_patient.pat_key
        inner join {{ ref('dim_date') }} as dim_date
            on stg_encounter.encounter_date = dim_date.full_date
        left join {{ref('stg_encounter_ed')}} as stg_encounter_ed
            on stg_encounter_ed.visit_key = stg_encounter.visit_key
    where
        dim_date.fiscal_year between 2020 and 2022
),

ed_pat_lvl as (
    select
        ed_visit_lvl.pat_key,
        ed_visit_lvl.race_ethnicity,
        ed_visit_lvl.fiscal_year,
        sum(ed_visit_lvl.numerator) as numerator,
        1 as denominator
    from
        ed_visit_lvl
    group by
        ed_visit_lvl.pat_key,
        ed_visit_lvl.race_ethnicity,
        ed_visit_lvl.fiscal_year
)

select
    stg_equity_geos_pivot.subdiv_type,
    stg_equity_geos_pivot.subdiv_code,
    ed_pat_lvl.race_ethnicity,
    ed_pat_lvl.fiscal_year,
    sum(ed_pat_lvl.numerator) as numerator,
    sum(ed_pat_lvl.denominator) / 1000 as denominator
from
    ed_pat_lvl
    inner join {{ ref('stg_equity_geos_pivot') }} as stg_equity_geos_pivot
        on ed_pat_lvl.pat_key = stg_equity_geos_pivot.pat_key
group by
    stg_equity_geos_pivot.subdiv_type,
    stg_equity_geos_pivot.subdiv_code,
    ed_pat_lvl.race_ethnicity,
    ed_pat_lvl.fiscal_year
