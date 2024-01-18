/* stg_nursing_cost_center_leader
for the alignments of directors and VP level people assigned to cost centers for Nursing &
Clinical Care Services (NCCS) data visualizations, capturing the ones with RN reports to support
which applicable button(s) the leader's name should fall under
*/

with
cc_leader_rollup as (
    select
        cost_center_id,
        vp_worker_id,
        director_id
	from
        {{ ref('lookup_nursing_nccs_rollup') }}
    where
        vp_worker_id is not null
        or director_id is not null
),

cc_leader as (
    select
        cost_center_id,
        vp_worker_id as cc_superior_id
    from cc_leader_rollup

    union

    select
        cost_center_id,
        director_id as cc_superior_id
    from cc_leader_rollup
),

rpts_counts as (
    select
        superior_emp_key  /*, superior_org_key */,
        superior_lvl,
        sum(worker_active_ind) as active_worker_in_supv_org_cnt,
        sum(worker_active_rn_job_ind) as active_rn_in_supv_org_cnt,
        count(*) as total_subordinate_cnt,
        superior_worker_id
    from
        {{ ref('worker_subordinate_superior') }}
    group by
        superior_emp_key,
        superior_lvl,
        superior_worker_id
),

cc_person_data as (
    select
        worker.worker_id,
        worker.cost_center_id,
        worker.legal_reporting_name as leader_name,
        worker.magnet_reporting_ind,
        nursing_worker.nurse_manager_ind,
        nursing_worker.nurse_supervisor_ind,
        worker.management_level as leader_management_level,
        worker.reporting_chain as leader_reporting_chain,
        case
            when worker.rn_job_ind = 1
                and worker.active_ind = 1
            then 1
            else 0 end as leader_rn_job_ind
	from
        {{ ref('worker') }} as worker
        inner join {{ ref('nursing_worker') }} as nursing_worker
            on worker.worker_id = nursing_worker.worker_id
)

select
    cc_person_data.leader_reporting_chain,
    cc_leader.cc_superior_id,
    cc_leader.cost_center_id,
    cc_person_data.leader_name as cc_superior_name,
    cc_person_data.leader_management_level, rpts_counts.active_worker_in_supv_org_cnt,
    cc_person_data.leader_rn_job_ind,
    case
        when rpts_counts.active_rn_in_supv_org_cnt > 0
        then 1 else 0 end as superior_has_rn_reports_ind,
    case
        when superior_has_rn_reports_ind = 1
		then cc_person_data.leader_rn_job_ind
		else null end as superior_w_rn_in_rn_job_ind,
    case
        when cc_person_data.nurse_supervisor_ind + cc_person_data.nurse_manager_ind > 1
        then 1 else 0 end as nurse_supervisor_or_manager_job_ind,
	case
        when nurse_supervisor_or_manager_job_ind = 1
        then cc_person_data.leader_name end as cc_nurse_mid_management_nm,
	case
        when nurse_supervisor_or_manager_job_ind = 0
            and superior_has_rn_reports_ind = 1
        then cc_person_data.leader_name end as cc_nccs_leader_nm,
	case
        when nurse_supervisor_or_manager_job_ind = 0
            and superior_has_rn_reports_ind = 1
            and cc_person_data.magnet_reporting_ind = 1
        then cc_person_data.leader_name end as cc_nurse_upper_management_nm,
    lookup_management_level.management_level_sort_num as cc_superior_management_level_sort_num,
    lookup_management_level.management_level_abbr as cc_superior_management_level_abbreviation
from
    cc_leader
    inner join rpts_counts
        on cc_leader.cc_superior_id = rpts_counts.superior_worker_id
    inner join cc_person_data
        on rpts_counts.superior_worker_id = cc_person_data.worker_id
    left join {{ ref('lookup_management_level') }} as lookup_management_level
        on cc_person_data.leader_management_level = lookup_management_level.management_level
where
    superior_has_rn_reports_ind = 1
