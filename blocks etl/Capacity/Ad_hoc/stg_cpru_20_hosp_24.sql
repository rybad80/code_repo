{{ config(meta = {
    'critical': true
}) }}

with adt_department as (
	select
		visit_key,
		visit_event_key,
		pat_key,
		adt_event_id,
		eff_event_dt,
		visit_event.dept_key,
		department.dept_id,
		dict_pat_svc_key,
		dict_adt_event_key,
		dict_adt_event.src_id as adt_event_type_id,
		dict_adt_event.dict_nm as adt_event_type,
		lag(visit_event.dept_key) over (
			partition by visit_key order by eff_event_dt, adt_event_id desc
		) as prev_dept_key,
		case when
			(visit_event.dept_key != prev_dept_key or prev_dept_key is null)
			then 1 else 0
		end as new_dept_ind,
		max(
			case
				when dict_adt_event.src_id = 2
					and department.dept_id = 66
			then 1
			else 0
		end) over(partition by visit_key) as cpru_discharge_ind
	from
		{{source('cdw','visit_event')}} as visit_event
		inner join {{source('cdw','department')}} as department
			on department.dept_key = visit_event.dept_key
		inner join {{source('cdw','cdw_dictionary')}} as dict_adt_event
			on dict_adt_event.dict_key = visit_event.dict_adt_event_key
		inner join {{source('cdw','cdw_dictionary')}} as dict_event_subtype
			on dict_event_subtype.dict_key = visit_event.dict_event_subtype_key
	where
		dict_adt_event.src_id in (1, 2, 3) -- (Admission, Discharge, Transfer In)
		and dict_event_subtype.src_id != 2 -- Canceled
),

cpru_inpatient as (
	select
		adt_department.visit_key,
		stg_encounter.csn as pat_enc_csn_id,
		adt_department.visit_event_key,
		adt_department.dept_id as department_id,
		adt_department.pat_key,
		stg_encounter.pat_id,
		adt_department.adt_event_id,
		adt_department.dept_key,
		adt_department.dict_pat_svc_key,
		adt_department.dict_adt_event_key,
		adt_department.adt_event_type_id,
		adt_department.adt_event_type,
		eff_event_dt as dept_enter_date,
		lead(eff_event_dt, 1, hospital_discharge_date) over (
				partition by adt_department.visit_key order by eff_event_dt, adt_event_id desc
		) as dept_exit_date,
		coalesce(dept_exit_date, current_date) as dept_exit_date_or_current_date,
		extract( --noqa: PRS
			epoch from dept_exit_date_or_current_date - dept_enter_date
		) / 3600.0 as dept_los_hrs_as_of_today,
		case when extract( --noqa: PRS
				epoch from coalesce(hospital_discharge_date, current_timestamp) - hospital_admit_date
				) > (24.0 * 60.0 * 60.0)
						and (
							adt_department.dept_id = 66
							and dept_los_hrs_as_of_today > 20
							and dept_enter_date < '2020-10-19'
						)
			then 1 else 0
		end as cpru_20_hosp_24_ind,
		cpru_discharge_ind
	from
		adt_department
		inner join {{ref('stg_encounter')}} as stg_encounter
			on stg_encounter.visit_key = adt_department.visit_key
	where
		new_dept_ind = 1
)

select
	visit_key,
	pat_enc_csn_id,
	visit_event_key,
	department_id,
	pat_key,
	pat_id,
	adt_event_id,
	dept_key,
	dict_pat_svc_key,
	dict_adt_event_key,
	adt_event_type_id,
	adt_event_type,
	dept_enter_date,
	dept_exit_date,
	dept_los_hrs_as_of_today,
	coalesce(cpru_discharge_ind, 0) as cpru_discharge_ind
from
	cpru_inpatient
where
	cpru_20_hosp_24_ind = 1
