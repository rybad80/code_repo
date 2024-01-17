select
    cancer_center_patient.mrn,
    cancer_center_patient.pat_key,
    chopradiationcourse.firsttreatmentdate,
    cancer_center_patient.pat_key as primary_key,
    'onco_rad_onc_pat' as metric_id
from
   {{ source('ods','chopradiationcourse')}} as chopradiationcourse
   inner join
    {{ ref('cancer_center_patient')}} as cancer_center_patient
        on cancer_center_patient.mrn = strright('00' || chopradiationcourse.chopmrn, 8)
