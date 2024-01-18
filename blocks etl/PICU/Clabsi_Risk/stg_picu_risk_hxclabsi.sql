select cohort.pat_mrn_id,
	cohort.pat_key,
	census_dt,
	max(case when eventdate < date(census_dt) then 1 else 0 end) as hx_clabsi_ind

from {{ source('cdw.cdw_analytics', 'metrics_hai') }} as metrics_hai
		inner join {{ ref('stg_picu_central_line_cohort') }} as cohort
			on metrics_hai.pat_key = cohort.pat_key

where metrics_hai.hai_type = 'CLABSI'

group by 1, 2, 3
