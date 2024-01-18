select
    visit_key,
    mrn,
    1 as minds_matter_sde_ind
from
    {{ ref('smart_data_element_all') }}
where
    year(encounter_date) >= '2017'
    and lower(concept_id) in (
        'chop#1922',
        'chop#5298')
group by
    visit_key,
    mrn
