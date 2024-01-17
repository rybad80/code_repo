select
    visit_key,
    mrn,
    1 as minds_matter_visit_type_ind
from
    {{ ref('stg_encounter') }}
where
    year(encounter_date) >= '2017'
    and lower(visit_type) like '%concussion%'
