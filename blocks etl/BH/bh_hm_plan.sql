select
    patient_hmt_status.pat_id,
	patient_all.pat_key,
	hm_plan_info.hm_plan_id,
	hm_plan_info.hm_plan_name,
	hm_topic.hm_topic_id as hm_topic_id,
    hm_topic.name  as hm_topic_name,
	hmt_due_status.name as hmt_status,
	case when hmt_due_status.hmt_due_status_c not in (4, 10) then 1
	when hmt_due_status.hmt_due_status_c = 10 then null
	else 0 end as compliant_ind,
	patient_hmt_status.ideal_return_dt as hm_ideal_return_dt,
	patient_hmt_status.hmt_ppn_untl_dt,
	case when lower(hm_topic_name) like '%baseline%' then 1 else 0 end as baseline_ind
from    {{source('clarity_ods', 'patient_hmt_status')}}  as patient_hmt_status
inner join
    {{source('clarity_ods', 'hm_plan_info')}} as hm_plan_info on
        patient_hmt_status.active_hm_plan_id  = hm_plan_info.hm_plan_id
inner join
    {{source('clarity_ods', 'clarity_hm_topic')}} as hm_topic on
        hm_topic.hm_topic_id = patient_hmt_status.qualified_hmt_id
inner join
    {{source('clarity_ods', 'zc_hmt_due_status')}}  as hmt_due_status on
        hmt_due_status.hmt_due_status_c = patient_hmt_status.hmt_due_status_c
inner join {{ref('patient_all')}} as patient_all on patient_hmt_status.pat_id = patient_all.pat_id
where hm_plan_info.hm_plan_id in (15,
12,
10,
9,
7,
21,
13,
6
)
