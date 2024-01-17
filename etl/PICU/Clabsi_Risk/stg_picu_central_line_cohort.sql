select distinct
	p.pat_mrn_id,
	cl.pat_key,
	cl.lda_id,
	cl.place_dt,
	cl.line_grouping,
	cl.pat_lda_key,
	cl.visit_key,
	cl.hosp_admit_dt,
	adt.department_group_name as dept_grp_abbr,
	cl.census_dt,
	case when cl.line_grouping = 'Broviac' then 1 else 0 end as broviac_ind,
	count(distinct cl.lda_id) over (partition by p.pat_mrn_id, cl.census_dt) as num_lines,
	case when num_lines > 1 then 1 else 0 end as mult_line_ind
	from
		{{source('cdw.cdw_analytics', 'fact_ip_lda_central_line') }} as cl
		inner join {{ source('cdw', 'patient') }} as p on p.pat_key = cl.pat_key
        inner join {{ ref('encounter_inpatient') }} as inpt on inpt.visit_key = cl.visit_key
        inner join {{ ref('adt_department') }} as adt
			on adt.visit_key = cl.visit_key
            and adt.dept_key = cl.dept_key
    where
		date(cl.census_dt) >= '07/01/2018'
		and adt.department_group_name = 'PICU'
        and adt.department_center_id = 104
