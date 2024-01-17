select
    diagnosis_encounter_all.mrn
from {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on diagnosis_encounter_all.visit_key = stg_encounter.visit_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_frontier_program_providers_all
        --on stg_encounter.provider_id = cast(
        on provider.prov_id = cast(
            lookup_frontier_program_providers_all.provider_id as nvarchar(20))
        and lookup_frontier_program_providers_all.program = 'rare-lung'
    left join {{ ref('lookup_frontier_program_diagnoses')}} as lookup_frontier_program_diagnoses
        on diagnosis_encounter_all.icd10_code = lookup_frontier_program_diagnoses.lookup_dx_id
            and lookup_frontier_program_diagnoses.program = 'rare-lung'
where
    lower(diagnosis_encounter_all.diagnosis_name) = lookup_frontier_program_diagnoses.lookup_dx_label
    or ((diagnosis_encounter_all.icd10_code in ('R91.1', 'R91.8')
            and lower(diagnosis_encounter_all.diagnosis_name) like '%nodule%')
        or (diagnosis_encounter_all.icd10_code = 'M32.9'
            and lower(diagnosis_encounter_all.diagnosis_name) like 'systemic lupus erythematosus, unspeci%')
        or (diagnosis_encounter_all.icd10_code like 'M32%'
            and lower(diagnosis_encounter_all.diagnosis_name) like '%ystemic lupus erythematosus (sle)%'
            )
        )
group by
    diagnosis_encounter_all.mrn
