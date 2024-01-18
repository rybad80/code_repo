select
    year(add_months(encounter_specialty_care.encounter_date, 6)) as fiscal_year,
    master_date.fy_yyyy_qtr as fiscal_quarter,
    master_date.c_yyyy as calendar_year,
    encounter_specialty_care.*
from
    {{ ref('encounter_specialty_care') }} as encounter_specialty_care
    inner join {{ ref('ctis_registry') }} as ctis_registry
        on ctis_registry.pat_key = encounter_specialty_care.pat_key
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.full_dt = encounter_specialty_care.encounter_date
where
    lower(encounter_specialty_care.specialty_name) in ('orthopedics', 'pulmonary')

    --for counting op visits
