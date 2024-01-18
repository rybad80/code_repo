select
    'Program-Specific: International Patients' as metric_name,
    frontier_thyroid_encounter_cohort.visit_key as primary_key,
    initcap(patient.country) as drill_down_one,
    frontier_thyroid_encounter_cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_thyroid_international_pat' as metric_id,
    frontier_thyroid_encounter_cohort.pat_key as num
from
    {{ ref('frontier_thyroid_encounter_cohort')}} as frontier_thyroid_encounter_cohort
    inner join {{ ref('stg_encounter_chop_market')}}  as stg_encounter_chop_market
        on frontier_thyroid_encounter_cohort.visit_key = stg_encounter_chop_market.visit_key
        and stg_encounter_chop_market.chop_market = 'international'
    left join {{source('cdw', 'patient')}} as patient
        on frontier_thyroid_encounter_cohort.pat_key = patient.pat_key
