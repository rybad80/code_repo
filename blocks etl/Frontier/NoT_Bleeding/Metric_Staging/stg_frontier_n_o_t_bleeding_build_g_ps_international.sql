select
    'Program-Specific: International Patients' as metric_name,
    cohort.visit_key as primary_key,
    initcap(patient.country) as drill_down_one,
    cohort.encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_n_o_t_bleeding_international_pat' as metric_id,
    cohort.pat_key as num
from
    {{ ref('frontier_n_o_t_bleeding_encounter_cohort')}} as cohort
    inner join {{ ref('stg_encounter_chop_market')}}  as stg_encounter_chop_market
        on cohort.visit_key = stg_encounter_chop_market.visit_key
        and stg_encounter_chop_market.chop_market = 'international'
    inner join {{source('cdw', 'patient')}} as patient
        on cohort.pat_key = patient.pat_key
