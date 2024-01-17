select
    lookup_bioresponse_diagnosis.diagnosis_hierarchy_1,
    lookup_bioresponse_diagnosis.normal_infectious_window,
    lookup_bioresponse_diagnosis.max_infectious_window
from
    {{ ref('lookup_bioresponse_diagnosis') }} as lookup_bioresponse_diagnosis
group by
    lookup_bioresponse_diagnosis.diagnosis_hierarchy_1,
    lookup_bioresponse_diagnosis.normal_infectious_window,
    lookup_bioresponse_diagnosis.max_infectious_window
