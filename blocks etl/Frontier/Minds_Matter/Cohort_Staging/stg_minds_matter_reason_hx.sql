select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    1 as minds_matter_reason_visit_ind
from
    {{ ref('stg_encounter') }} as stg_encounter
    left join {{ source('cdw', 'visit_reason') }} as visit_reason
        on stg_encounter.visit_key = visit_reason.visit_key
    inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on stg_encounter.visit_key = diagnosis_encounter_all.visit_key
            and diagnosis_encounter_all.visit_diagnosis_ind = 1
    inner join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
        on visit_reason.rsn_key = master_reason_for_visit.rsn_key
where
    regexp_like(lower(master_reason_for_visit.rsn_nm),
        'concus|
        |commotio.*cerebri|
        |post.*traumatic.*brain|
        |brain.*post.*traumatic|
        |boxer.*dementia|
        |punch.*drunk|
        |dementia.*pugilistica|
        |head injury'
        )
group by
    stg_encounter.visit_key,
    stg_encounter.mrn
