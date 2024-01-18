select
    smart_data_element_all.mrn
from
    {{ ref('smart_data_element_all') }} as smart_data_element_all
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on smart_data_element_all.mrn = stg_encounter.mrn
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_frontier_program_providers_all
        --on stg_encounter.provider_id = cast(
        on provider.prov_id = cast(
            lookup_frontier_program_providers_all.provider_id as nvarchar(20))
        and lookup_frontier_program_providers_all.program = 'rare-lung'
where
    lower(concept_description) = 'chop rare lung smarttext'
group by
    smart_data_element_all.mrn
