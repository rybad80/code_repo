with ostomy_gran as (select distinct
    pat_mrn_id,
	cohort.pat_key,
	1 as ostomy_ind,
    census_dt,
	date(census_dt) - date(flow_mea.rec_dt) as time_to_census,
	case when time_to_census between 0 and 3 then 1 else 0 end as keep_ind
from {{ source('cdw', 'visit_addl_info') }} as vai
	inner join {{ ref('stg_picu_central_line_cohort') }} as cohort on cohort.visit_key = vai.visit_key
    inner join {{ source('cdw', 'flowsheet_record') }} as flow_rec on flow_rec.vsi_key = vai.vsi_key
    inner join {{ source('cdw', 'flowsheet_measure') }} as flow_mea on flow_mea.fs_rec_key = flow_rec.fs_rec_key
    inner join {{ source('cdw', 'flowsheet') }} as flow on flow.fs_key = flow_mea.fs_key

where fs_id = 40068057 and keep_ind = 1
)

select distinct pat_mrn_id, pat_key, census_dt, ostomy_ind
from ostomy_gran
