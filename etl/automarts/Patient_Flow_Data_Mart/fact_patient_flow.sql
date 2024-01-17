/*
Author: Sara Chapman
Contents: Patient flow information from ADT events for patients admitted after 1/1/2013
Granularity: 1 row per department entry per visit (bed transfers have been removed)
*/

with depts_raw as (
--region
select
	visit.visit_key,
	patient.pat_key,
	visit.hosp_dischrg_dt as hosp_disch_dt,
	ve.eff_event_dt as enter_dt,
	dept.dept_key,
	floor(dept.dept_id) as dept_id,
	dept.dept_nm,
	coalesce(
		dept_groups_by_date.chop_dept_grp_abbr,
		dept_groups_imputation.chop_dept_grp_abbr
	) as dept_grp_abbr,
	lag(dept.dept_key) over(partition by visit.visit_key order by ve.eff_event_dt) as prev_dept_key,
	case when dept.dept_key != prev_dept_key or prev_dept_key is null then 1 else 0 end as new_dept_ind

from {{ source('cdw', 'visit' ) }} as visit
	inner join {{ source('cdw', 'patient' ) }} as patient on patient.pat_key = visit.pat_key
	inner join {{ source('cdw', 'visit_event' ) }} as ve  on ve.visit_key = visit.visit_key
	inner join {{ source('cdw', 'cdw_dictionary' ) }} as d_enter on d_enter.dict_key = ve.dict_adt_event_key
	inner join {{ source('cdw', 'cdw_dictionary' ) }} as d_status on d_status.dict_key = ve.dict_event_subtype_key
	inner join {{ source('cdw', 'department' ) }} as dept on dept.dept_key = ve.dept_key
	left join {{source('cdw_analytics','fact_department_rollup_summary')}} as dept_groups_by_date
		on dept.dept_key = dept_groups_by_date.dept_key
		and ve.eff_event_dt between dept_groups_by_date.min_dept_align_dt and dept_groups_by_date.max_dept_align_dt
	left join {{source('cdw_analytics','fact_department_rollup_summary')}} as dept_groups_imputation
		on dept.dept_key = dept_groups_imputation.dept_key
		and dept_groups_imputation.min_dept_align_dt = '2014-01-01'

where
	d_enter.src_id in (1, 3)
	and d_status.dict_nm != 'Canceled'
	and visit.hosp_admit_dt >= '1/1/2012'

group by
	visit.visit_key,
	patient.pat_key,
	visit.hosp_dischrg_dt,
	ve.eff_event_dt,
	dept.dept_key,
	dept.dept_id,
	dept.dept_nm,
	dept_groups_by_date.chop_dept_grp_abbr,
	dept_groups_imputation.chop_dept_grp_abbr
--endregion
)

select
	visit_key,
	pat_key,
	dept_key,
	dept_id,
	dept_nm,
	dept_grp_abbr,
	enter_dt,
	lead(enter_dt, 1, hosp_disch_dt) over(partition by visit_key order by enter_dt) as exit_dt,
	extract(epoch from exit_dt - enter_dt) / 3600.0 as los_hrs,

	row_number() over(partition by visit_key order by enter_dt) as dept_seq_num,

	case when dept_seq_num = 1 then 1 else 0 end as first_dept_ind,
	case
        when row_number() over(partition by visit_key order by enter_dt desc) = 1 then 1 else 0
	end as last_dept_ind

from depts_raw

where new_dept_ind = 1
