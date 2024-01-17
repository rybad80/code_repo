select
    visit_key,
    mrn,
    1 as visit_hx_ind
from
    {{ ref('stg_encounter') }}
where
    lower(visit_type) in (
        'follow up rare lung',
        'new rare lung',
        'video visit fol up rare lung'
        )
    and year(add_months(encounter_date, 6)) >= 2022
    and encounter_date < current_date
