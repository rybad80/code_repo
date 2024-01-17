select
    oncore_diagnosis.pat_key,
    max(case when lower(oncore_diagnosis.dx_diagnosis_type) = 'new diagnosis' then 1 else 0 end) as new_dx_ind,
    max(case when stg_cancer_center_chemo_rad.pat_key is not null then 1 else 0 end) as received_chemo_or_rad_ind,
    case when received_chemo_or_rad_ind = 1
        then 1 else 0 end as oncore_criteria_ind
from
    {{source('cdw', 'oncore_diagnosis')}} as oncore_diagnosis
    left join {{ref ('stg_cancer_center_chemo_rad')}} as stg_cancer_center_chemo_rad
        on oncore_diagnosis.pat_key = stg_cancer_center_chemo_rad.pat_key
group by
    oncore_diagnosis.pat_key
