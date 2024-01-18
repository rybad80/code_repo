with bhconsult_granularity as (select distinct
cohort.pat_mrn_id,
cohort.pat_key,
po.visit_key,
proc_ord_create_dt,
d.dict_nm,
po.canc_rsn_cmt,
po.cancel_dt,
census_dt,
1 as bh_consult_ind,
date(census_dt) - date(proc_ord_create_dt) as time_to_census,
case when time_to_census between 0 and 90 then 1 else 0 end as keep_ind
from {{ ref('stg_picu_central_line_cohort') }} as cohort
inner join {{ source('cdw', 'patient') }} as pat on pat.pat_key = cohort.pat_key
inner join {{ source('cdw', 'procedure_order') }} as po on po.pat_key = pat.pat_key
inner join {{ source('cdw', 'procedure') }} as proc on proc.proc_key = po.proc_key --noqa: L029
inner join {{ source('cdw', 'cdw_dictionary') }} as d on d.dict_key = po.dict_ord_stat_key
where po.proc_ord_create_dt <= '20180101'
	and proc_cd in('9035.001', --	'CONSULT TO PSYCHIATRY (CHOP)'
				'9036.001', --	'CONSULT TO PSYCHOLOGY (CHOP)'
				'9104.001', --	'CONSULT TO BEHAVIORAL HEALTH (CHOP)'
			'9313.001') --	'CONSULT TO BEHAVIORAL HEALTH- INPATIENT ONLY'
)

select pat_mrn_id, pat_key, census_dt, max(bh_consult_ind) as bh_consult_ind
from bhconsult_granularity
where keep_ind = 1
group by 1, 2, 3
