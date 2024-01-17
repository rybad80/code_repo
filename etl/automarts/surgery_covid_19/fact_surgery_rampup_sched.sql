with surg_pred as ( -- region expected LOS in order
	select
	or_case.or_case_id,
	m_quest.latest_display_quest_nm,
	m_quest.quest_nm,
	case when ansr = 100 then '< 23 Hours'
		when ansr = 105 then '23 - 48 Hours'
		when ansr = 110 then '48 - 72 Hours'
		when ansr = 115 then '> 72 Hours'
		else null end as questionnaire_los

	from {{ source('cdw', 'order_question') }} as ord_quest
	inner join {{ source('cdw', 'master_question') }} as m_quest on ord_quest.quest_key = m_quest.quest_key
	inner join {{ source('cdw', 'order_xref') }} as ord_x on ord_x.ord_key = ord_quest.ord_key
	inner join {{ source('cdw', 'or_case_order') }} as orc_order on orc_order.ord_key = ord_quest.ord_key
	inner join {{ source('cdw', 'or_case') }} as or_case on orc_order.or_case_key = or_case.or_case_key

	where
		m_quest.quest_id = '900100174' --CHOP OPT EXPECTED LENGTH OF STAY
	--end region
)

select
fact_periop_sched_cases.sched_surgery_dt,
fact_periop_sched_cases.or_case_key,
fact_periop_sched_cases.or_case_id,
fact_periop_sched_cases.pat_key,
patient.zip,
patient.full_nm,
patient.pat_mrn_id,
patient.county,
fact_periop_sched_cases.sched_start_dt,
fact_periop_sched_cases.sched_end_dt,
fact_periop_sched_cases.loc,
fact_periop_sched_cases.room,
fact_periop_sched_cases.service,
fact_periop_sched_cases.proc_primary,
or_case.tot_tm_needed,
dict_loc.dict_nm as postop_dest_surg,
fact_periop_sched_cases.final_pat_dest,
COALESCE(final_pat_dest, postop_dest_surg) as postop_dest_combined,
dict_priority.dict_nm as priority,
surg_pred.questionnaire_los,
case when visit.hosp_admit_dt < fact_periop_sched_cases.sched_surgery_dt then 1 else 0 end as ip_ind

from {{ ref('fact_periop_sched_cases') }} as fact_periop_sched_cases
inner join {{ source('cdw', 'or_case') }} as or_case on or_case.or_case_key = fact_periop_sched_cases.or_case_key
inner join {{ source('cdw', 'cdw_dictionary') }} as dict_loc on dict_loc.dict_key = or_case.dict_or_post_dest_key
inner join
    {{ source('cdw', 'cdw_dictionary') }} as dict_priority on dict_priority.dict_key = or_case.dict_or_prty_key
inner join {{ source('cdw', 'patient') }} as patient on patient.pat_key = fact_periop_sched_cases.pat_key
inner join {{ source('cdw', 'visit') }} as visit on visit.visit_key = or_case.admit_visit_key
left join surg_pred on surg_pred.or_case_id = or_case.or_case_id

where sched_surgery_dt >= '2020-05-01'
--and loc not in  ('CARDIAC OPERATIVE IMAGING COMPLEX', 'SPECIAL DELIVERY UNIT')
