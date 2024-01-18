select
    registry_tumor_oncology.pat_key,
    max(case when onco_general_dx_cd in (
        10,
        20,
        21,
        30,
        40,
        41,
        50,
        60,
        70,
        75,
        76,
        80,
        81,
        82
        ) then 1 else 0 end) as malignant_dx_ind,
    max(case when strright(coalesce(icdo_histology_behavior_cd, '0'), 1) = '3'
        then 1 else 0 end) as histology_behavior_ind,
    max(case when stg_cancer_center_chemo_rad.pat_key is not null
        then 1 else 0 end) as received_chemo_or_rad_ind,
    case when malignant_dx_ind = 1
        and histology_behavior_ind = 1
        and received_chemo_or_rad_ind = 1
        then 1 else 0 end
        as tumor_registry_criteria_ind
from
    {{source('cdw', 'registry_tumor_oncology')}} as registry_tumor_oncology
    left join {{ref ('stg_cancer_center_chemo_rad')}} as stg_cancer_center_chemo_rad
        on registry_tumor_oncology.pat_key = stg_cancer_center_chemo_rad.pat_key
group by
    registry_tumor_oncology.pat_key
