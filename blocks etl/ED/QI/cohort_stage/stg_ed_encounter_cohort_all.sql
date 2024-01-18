select
    fact_edqi.visit_key,
    fact_edqi.pat_key,
    fact_edqi.enc_id,
    fact_edqi.arrive_ed_dt,
    fact_edqi.disch_ed_dt,
    fact_edqi.depart_ed_dt,
    fact_edqi.admit_edecu_dt,
    fact_edqi.disch_edecu_dt,
    'ED_ALL' as cohort,
    null as subcohort
from
    {{ source('cdw_analytics', 'fact_edqi') }} as fact_edqi
    inner join
        {{ ref('stg_encounter') }} as stg_encounter          on stg_encounter.visit_key = fact_edqi.visit_key
