select
    'Program Specific: OCC/P-Therapy' as metric_name,
    visit_key as primary_key,
    department_name as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_minds_matter_occt_n_pt' as metric_id,
    pat_key as num
from
    {{ ref('frontier_minds_matter_encounter_cohort') }}
where
    minds_matter_pt_occt_ind = '1'
    and visit_type_id != '0'
