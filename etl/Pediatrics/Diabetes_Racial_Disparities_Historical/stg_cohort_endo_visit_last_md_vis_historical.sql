select
    pat_key,
    endo_vis_dt
from
    {{ ref('stg_cohort_endo_visit_historical')}}
group by
    pat_key,
    endo_vis_dt
