select
	stg_cohort_endo_visit.patient_key,
	stg_cohort_endo_visit.endo_vis_dt
from
    {{ ref('stg_cohort_endo_visit') }} as stg_cohort_endo_visit
group by
	stg_cohort_endo_visit.patient_key,
	stg_cohort_endo_visit.endo_vis_dt
