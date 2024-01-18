select
    lookup_bioresponse_diagnosis.diagnosis_hierarchy_1,
    lookup_diagnosis_icd10.icd10_code
from
    {{ ref('lookup_diagnosis_icd10') }} as lookup_diagnosis_icd10
    inner join {{ ref('lookup_bioresponse_diagnosis') }} as lookup_bioresponse_diagnosis
        on regexp_like(
            lookup_diagnosis_icd10.icd10_code,
            lookup_bioresponse_diagnosis.diagnosis_pattern
        )
        and lookup_bioresponse_diagnosis.icd_version = 'icd10'
