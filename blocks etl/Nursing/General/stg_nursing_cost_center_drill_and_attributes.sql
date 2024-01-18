{{ config(meta = {
    'critical': true
}) }}

--  b. stg_nursing_cost_center_drill_and_attributes built
--  from stg_nursing_cost_center_in_hierarchy and worker counts
/* stg_nursing_cost_center_drill_and_attributes
from cost_center Workday hierarchy data and worker counts
set the attributes for cost centers from the nursing perspective and for
drilling down through the main All Cost Centers hierarchy
*/
with cost_centers_in_hierarchy as (
    select
        cost_cntr_key,
        cost_cntr_id,
        cost_cntr_nm,
        cc_active_ind,
        parent_level,
        lvl_1_cc_hier_nm,
        lvl_2_cc_hier_nm,
        lvl_3_cc_hier_nm,
        lvl_4_cc_hier_nm,
        lvl_5_cc_hier_nm,
        lvl_6_cc_hier_nm,
        lvl_2_cost_cntr_hierarchy_nm,
        lvl_3_cost_cntr_hierarchy_nm,
        lvl_4_cost_cntr_hierarchy_nm,
        lvl_5_cost_cntr_hierarchy_nm,
        lvl_6_cost_cntr_hierarchy_nm,
        parent_of_cost_center
    from
        {{ ref('stg_nursing_cost_center_finance_hierarchy') }}
        /* 'All Cost Centers' */
),

alternate_cost_center_hierarchy as (
    select
        cost_cntr_key,
        cost_cntr_nm,
        parent_level,
        lvl_1_cc_hier_nm,
        lvl_2_cc_hier_nm,
        lvl_3_cc_hier_nm
    from {{ ref('stg_nursing_cost_center_in_hierarchy') }}
    where lvl_1_cc_hier_nm != 'All Cost Centers'  -- not the ones that goes down many levels
),

surgery_center as (
    select
        cost_cntr_key as alt_hier_cost_cntr_key,
        lvl_1_cc_hier_nm as surgery_center_cc_hier_nm
    from alternate_cost_center_hierarchy
    where lvl_1_cc_hier_nm like 'ASC%'
),

hospital_ccs as (
    select
        cost_cntr_key as alt_hier_cost_cntr_key,
        lvl_1_cc_hier_nm as hospital_cc_hier_1_nm,
        lvl_2_cc_hier_nm as hospital_cc_hier_2_nm
    from alternate_cost_center_hierarchy
    where lvl_1_cc_hier_nm = 'Total Hospital Reporting'
),

care_network as (
    select
        cost_cntr_key as alt_hier_cost_cntr_key,
        lvl_1_cc_hier_nm as care_network_cc_hier_nm
    from alternate_cost_center_hierarchy
    where lvl_1_cc_hier_nm like 'Care Network%'
),

nursing_grouper as (
    select
    cost_cntr_key,
    case
    when lvl_3_cc_hier_nm = 'Clinical Services' then
        case
        when lvl_5_cc_hier_nm = 'Nursing Surgical Services' then
            case
                when surgery_center.surgery_center_cc_hier_nm is null then 'Hospital Surgery'
                else 'Ambulatory Surgery Center' end
        when lvl_5_cc_hier_nm = 'Nursing Anesthesia Services' then
            case
                when surgery_center.surgery_center_cc_hier_nm > '' then 'Ambulatory Surgery Center'
                else 'Hospital Surgery' end /* includes PACU */
        when lvl_4_cc_hier_nm = 'Nursing' then
            case
            when lvl_5_cc_hier_nm = 'Nursing Operations Support' then lvl_5_cc_hier_nm
            else 'Acute Nursing Unit' end
		when lvl_4_cc_hier_nm = 'Ancillary Services' then lvl_4_cc_hier_nm
        else 'Acute Other'
        end

    when lvl_3_cc_hier_nm = 'Network Operations' then
        case
        when lvl_4_cc_hier_nm = '' then
            'Network Ops TBD'
        else lvl_4_cc_hier_nm /* 'Ambulatory : Primary/Specialty/Urgent' */
        end

    when lvl_2_cc_hier_nm in ('Enterprise Services', 'CHOPPA', 'Research')
    then 'Other ' || lvl_2_cc_hier_nm
    when lvl_3_cc_hier_nm in ('Home Care Operations', 'Hospital Operations', 'Foundation Administration')
    then 'Other ' || lvl_3_cc_hier_nm
    else 'Other - catg TBD'
    end as nursing_cost_center_group
    from
        cost_centers_in_hierarchy
        left join surgery_center
            on cost_centers_in_hierarchy.cost_cntr_key = surgery_center.alt_hier_cost_cntr_key
),

worker_counts as (
    select
        sum(w.active_ind) as cc_active_worker_cnt,
        sum(w.active_ind * w.magnet_reporting_ind) as cc_active_magnet_worker_cnt,
        sum(w.active_ind * w.rn_job_ind) as cc_active_rn_cnt,
        w.cost_center_id
    from {{ ref('worker') }} as w
    group by w.cost_center_id
)

select
    worker_counts.cc_active_worker_cnt,
    worker_counts.cc_active_magnet_worker_cnt,
    worker_counts.cc_active_rn_cnt,
    cch.cost_cntr_id as cost_center_id,
    cch.cost_cntr_nm as cost_center_name,
    coalesce(cc_type.nursing_cost_center_type, 'Support') as cost_center_type,
    cc_grp.nursing_cost_center_group as cost_center_group,
    cch.parent_level,
    cch.parent_of_cost_center as cost_center_parent,
    cch.cost_cntr_id  || ' ' || cch.cost_cntr_nm as cost_center_display,
    cch.cc_active_ind,
    cch.lvl_1_cc_hier_nm,
    cch.lvl_2_cc_hier_nm,
    cch.lvl_3_cc_hier_nm,
    cch.lvl_4_cc_hier_nm,
    cch.lvl_5_cc_hier_nm,
    cch.lvl_6_cc_hier_nm,
    'All'
        || case when cch.lvl_2_cc_hier_nm = '' then '' else ' -> ' || cch.lvl_2_cc_hier_nm end
        || case when cch.lvl_3_cc_hier_nm = '' then '' else ' -> ' || cch.lvl_3_cc_hier_nm end
        || case when cch.lvl_4_cc_hier_nm = '' then '' else ' -> ' || cch.lvl_4_cc_hier_nm end
		|| case when cch.lvl_5_cc_hier_nm = '' then '' else ' -> ' || cch.lvl_5_cc_hier_nm end
		|| case when cch.lvl_6_cc_hier_nm = '' then '' else ' -> ' || cch.lvl_6_cc_hier_nm end
    as full_hierarchy_level_path,
    cch.lvl_1_cc_hier_nm as drill_cc_l1,
    case when cch.lvl_2_cc_hier_nm = '' then '' else '..' || cch.lvl_2_cc_hier_nm end
        as drill_cc_l2,
    case when cch.lvl_3_cc_hier_nm = '' then '' else '....' || cch.lvl_3_cc_hier_nm end
        as drill_cc_l3,
    case when cch.lvl_4_cc_hier_nm = '' then '' else '......' || cch.lvl_4_cc_hier_nm end
        as drill_cc_l4,
    case when cch.lvl_5_cc_hier_nm = '' then '' else '........' || cch.lvl_5_cc_hier_nm end
        as drill_cc_l5,
    case when cch.lvl_6_cc_hier_nm = '' then '' else '..........' || cch.lvl_6_cc_hier_nm end
        as drill_cc_l6,
    case when cch.lvl_2_cc_hier_nm = '' then '' else 'all' || ' > ' || cch.lvl_2_cc_hier_nm end
        as drill_cc_l2_path,
    case when cch.lvl_3_cc_hier_nm = '' then '' else drill_cc_l2_path || ' > ' || cch.lvl_3_cc_hier_nm end
        as drill_cc_l3_path,
    case when cch.lvl_4_cc_hier_nm = '' then '' else drill_cc_l3_path || ' > ' || cch.lvl_4_cc_hier_nm end
        as drill_cc_l4_path,
    case when cch.lvl_5_cc_hier_nm = '' then '' else drill_cc_l4_path || ' > ' || cch.lvl_5_cc_hier_nm end
        as drill_cc_l5_path,
    case when cch.lvl_6_cc_hier_nm = '' then '' else drill_cc_l5_path || ' > ' || cch.lvl_6_cc_hier_nm end
        as drill_cc_l6_path,
	coalesce(
        case when drill_cc_l6_path = '' then cast(null as varchar(20)) else drill_cc_l6_path end,
        case when drill_cc_l5_path = '' then cast(null as varchar(20)) else drill_cc_l5_path end,
        case when drill_cc_l4_path = '' then cast(null as varchar(20)) else drill_cc_l4_path end,
        case when drill_cc_l3_path = '' then cast(null as varchar(20)) else drill_cc_l3_path end,
        case when drill_cc_l2_path = '' then cast(null as varchar(20)) else drill_cc_l2_path end,
        case when lvl_1_cc_hier_nm = '' then cast(null as varchar(20)) else 'all'
    end) as full_drill_cc_path,
    case when surgery_center.alt_hier_cost_cntr_key is null then 0 else 1 end as surgery_center_cc_ind,
    surgery_center.surgery_center_cc_hier_nm as surgery_center_cc_level_name,
    case when hospital_ccs.alt_hier_cost_cntr_key is null then 0 else 1 end as hospital_cc_ind,
    hospital_ccs.hospital_cc_hier_1_nm as hospital_cc_level_1_name,
    hospital_ccs.hospital_cc_hier_2_nm as hospital_cc_level_2_name,
    case when care_network.alt_hier_cost_cntr_key is null then 0 else 1 end as care_network_cc_ind,
    care_network.care_network_cc_hier_nm as care_network_cc_level_name,
    case
        when cch.lvl_5_cc_hier_nm = 'Room and Board' then
        /* KOP does not have room & board groups but needs its own category for Nursing reporting */
        case when cch.lvl_6_cc_hier_nm = '' then 'KOP ' || cch.lvl_5_cc_hier_nm
        else cch.lvl_6_cc_hier_nm end
    end as room_and_board_rollup,
    case
        when cch.lvl_5_cc_hier_nm = 'Room and Board' then room_and_board_rollup
        when cch.lvl_5_cc_hier_nm = 'Observation' then 'Observation Unit'
    end as hppd_rollup,
    case
        when hppd_rollup like '% Room and Board' then replace(hppd_rollup, ' Room and Board', '')
        when hppd_rollup like 'Observation Unit' then 'Obs unit'
        else hppd_rollup
	end as hppd_rollup_short_name,
    case when room_and_board_rollup is not null then 1 else 0 end as room_and_board_ind,
    case when hppd_rollup is not null then 1 else 0 end as hppd_ind,
    /* hours per unit of service is coming , HPPD is a subset , others coming later */
    coalesce(hppd_rollup, cost_center_parent, 'TBD') as whuos_rollup,
    case when cc_active_worker_cnt > 0 then 1 else 0 end as cc_has_active_workers_ind,
    case when cc_active_rn_cnt > 0 then 1 else 0 end as cc_has_active_rns_ind
from
    cost_centers_in_hierarchy as cch
left join surgery_center as surgery_center
    on cch.cost_cntr_key = surgery_center.alt_hier_cost_cntr_key
left join hospital_ccs as hospital_ccs
    on cch.cost_cntr_key = hospital_ccs.alt_hier_cost_cntr_key
left join care_network as care_network
    on cch.cost_cntr_key = care_network.alt_hier_cost_cntr_key
left join worker_counts as worker_counts
    on cch.cost_cntr_id = worker_counts.cost_center_id
left join nursing_grouper as cc_grp
    on cch.cost_cntr_key = cc_grp.cost_cntr_key
left join {{ ref('lookup_nursing_cost_center_group') }} as cc_type
    on cc_grp.nursing_cost_center_group = cc_type.nursing_cost_center_group
