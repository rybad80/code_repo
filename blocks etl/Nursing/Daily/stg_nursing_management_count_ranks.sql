/* stg_nursing_management_count_ranks
for RNs at CHOP, count who they report to by cost center
in order to support getting the RN leaders for each cost center
and handle supervisors specifically, getting next level person also
*/
with
indiv_contrib_that_are_rns as (
    select worker_id as rn_worker_id
    from
        {{ ref('worker') }}
    where
        rn_job_ind = 1
        --and lower(management_level) = 'individual contributor'
),
rn_rpts_to as (
    select get_direct_supervisor.worker_id as rn_worker_id,
        worker.cost_center_id as worker_cost_center_id,
        get_direct_supervisor.superior_worker_id,
        ldr_to_4_chain.full_drill_mgmt_path as rn_mgr_path,
        ldr_to_4_chain.drill_mgmt_l04 as lvl4_leader,
        get_direct_supervisor.superior_management_level_abbr,
        worker.rn_job_ind as rpt_is_an_rn,
        next_up.superior_last_name as next_mgr_nm,
        next_up.superior_management_level_abbr as next_mgmt_lvl,
        next_up.superior_worker_id as next_mgmt_level_worker_id
    from
        indiv_contrib_that_are_rns as rn
        inner join {{ ref('worker_subordinate_superior') }}
            as get_direct_supervisor
            on get_direct_supervisor.direct_supervisor_ind = 1
            and get_direct_supervisor.worker_id = rn.rn_worker_id
        left join {{ ref('worker_subordinate_superior') }} as next_up
            on get_direct_supervisor.superior_lvl
            = next_up.superior_lvl + 1
            and next_up.worker_id = rn.rn_worker_id
            /* see who RN's supervisor reports to */
            and lower(get_direct_supervisor.superior_management_level_abbr)
            = 'supv/lead'
        inner join {{ ref('worker_management_chain_navigation') }} as ldr_to_4_chain
            on get_direct_supervisor.worker_id = ldr_to_4_chain.worker_id
            /* get the path to the rn leader */
        inner join {{ ref('worker') }} as worker
            on get_direct_supervisor.worker_id = worker.worker_id
            and worker.active_ind = 1 /* get cost center of worker */
),
cost_center_leader_cnt as (
    select sum(worker.active_ind * rn_job_ind) as active_rn_cnt,
        rn_rpts_to.worker_cost_center_id,
        rn_rpts_to.superior_worker_id,
        rn_rpts_to.next_mgmt_level_worker_id,
        rn_rpts_to.next_mgmt_lvl,
        getsort.management_level_sort_num
    from
        {{ ref('worker') }} as worker
        inner join rn_rpts_to
            on worker.worker_id = rn_rpts_to.rn_worker_id
        left join {{ ref('lookup_management_level') }} as getsort
            on  rn_rpts_to.next_mgmt_lvl = getsort.management_level_abbr
    where
        worker.active_ind = 1
    group by
        rn_rpts_to.worker_cost_center_id,
        superior_worker_id,
        next_mgmt_level_worker_id,
        rn_rpts_to.next_mgmt_lvl,
        getsort.management_level_sort_num
)

-- stg_nursing_management_count_ranks
select
    active_rn_cnt,
    worker_cost_center_id,
    superior_worker_id,
    next_mgmt_level_worker_id,
    dense_rank() over (partition by worker_cost_center_id order by active_rn_cnt desc,
        coalesce(next_mgmt_level_worker_id, '0') desc,
        /* take the managers before any supervisors if have same count of rn reports */
        coalesce(next_mgmt_level_worker_id, superior_worker_id)
        ) as recrank_most_rn_mgmt,
    case
        when next_mgmt_level_worker_id is not null then
            dense_rank() over (partition by
                worker_cost_center_id,
                case
                when next_mgmt_level_worker_id is null
                then 'keep' else 'remove' end
            order by active_rn_cnt desc,
                management_level_sort_num,
                superior_worker_id)
        end as recrank_most_rn_supv,
    case
        when next_mgmt_level_worker_id is  null then
            dense_rank() over (partition by
                worker_cost_center_id,
                case when next_mgmt_level_worker_id is null
                then 'keep' else 'remove' end
            order by active_rn_cnt desc,
                management_level_sort_num,
                superior_worker_id)
        end as recrank_most_rn_non_supv
from cost_center_leader_cnt
