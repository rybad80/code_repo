select
    diagnosis_encounter_all.visit_key,
    diagnosis_encounter_all.mrn,
    1 as minds_matter_dx_ind
from
    {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    left join {{ ref('stg_encounter') }} as stg_encounter
        on diagnosis_encounter_all.visit_key = stg_encounter.visit_key
    left join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
where
    year(diagnosis_encounter_all.encounter_date) >= '2017'
    and diagnosis_encounter_all.visit_diagnosis_ind = 1
    and (regexp_like(lower(diagnosis_encounter_all.icd10_code), 's06.0|f07.81')
        or (regexp_like(lower(diagnosis_encounter_all.icd10_code),
                    's00.83|
                    |s00.93|
                    |s09.90|
                    |g44.30|
                    |g44.31')
            and regexp_like(lower(diagnosis_encounter_all.diagnosis_name),
                    'head|
                    |parietal|
                    |occipit|
                    |orbital|
                    |crown|
                    |temple|
                    |brow|
                    |cephalohematoma|
                    |sequela'
                    )
            )
        )
group by
    diagnosis_encounter_all.visit_key,
    diagnosis_encounter_all.mrn
