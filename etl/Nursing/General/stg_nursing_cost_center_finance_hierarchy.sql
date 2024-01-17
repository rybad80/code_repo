{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_cost_center_finance_hierarchy
get hierarchy placement for cost center in the All Cost Centers hierarchy tree
*/
with get_parent_name as (
    select
        cost_center_hierarchy_wid as parent_hier_wid,
        cost_center_hierarchy_id as parent_hier_id,
        cost_center_hierarchy_name as parent_hier_nm,
		case
            when cost_center_hierarchy_level2_wid is null then 1
            when cost_center_hierarchy_level3_wid is null then 2
            when cost_center_hierarchy_level4_wid is null then 3
            when cost_center_hierarchy_level5_wid is null then 4
            when cost_center_hierarchy_level6_wid is null then 5
            else 6
        end as parent_level
    from
        {{ ref('dim_cost_center_hierarchy') }}
    group by
        cost_center_hierarchy_wid,
        cost_center_hierarchy_id,
        cost_center_hierarchy_name,
        parent_level
),

cc_in_hier as (
    select
        all_hier.cost_cntr_id,
        all_hier.cost_cntr_nm,
        all_hier.cost_center_hierarchy_level1_name as lvl_1_cc_hier_nm,
        /* make empty strings if needed to build path later without NULLs) */
        coalesce(all_hier.cost_center_hierarchy_level2_name, '') as lvl_2_cc_hier_nm,
        coalesce(all_hier.cost_center_hierarchy_level3_name, '') as lvl_3_cc_hier_nm,
        coalesce(all_hier.cost_center_hierarchy_level4_name, '') as lvl_4_cc_hier_nm,
        coalesce(all_hier.cost_center_hierarchy_level5_name, '') as lvl_5_cc_hier_nm,
        coalesce(all_hier.cost_center_hierarchy_level6_name, '') as lvl_6_cc_hier_nm,
        coalesce(all_hier.cost_center_hierarchy_level6_wid,
            all_hier.cost_center_hierarchy_level5_wid,
            all_hier.cost_center_hierarchy_level4_wid,
            all_hier.cost_center_hierarchy_level3_wid,
            all_hier.cost_center_hierarchy_level2_wid,
        all_hier.cost_center_hierarchy_level1_wid) as cost_center_hier_wid,
        all_hier.cost_center_hierarchy_level2_name as lvl_2_cost_cntr_hierarchy_nm,
        all_hier.cost_center_hierarchy_level3_name as lvl_3_cost_cntr_hierarchy_nm,
        all_hier.cost_center_hierarchy_level4_name as lvl_4_cost_cntr_hierarchy_nm,
        all_hier.cost_center_hierarchy_level5_name as lvl_5_cost_cntr_hierarchy_nm,
        all_hier.cost_center_hierarchy_level6_name as lvl_6_cost_cntr_hierarchy_nm
from
    {{ ref('dim_cost_center_hierarchy') }} as all_hier
where
    all_hier.cost_center_toplevel_hierarchy_id = 'CCH_Alll_Cost_Centers' /* ignore NA seed row */
)

select
    cc.cost_cntr_key, /* need to join to alternate cost center Hierarchies for now */
    cc_in_hier.cost_cntr_id,
    cc.cost_cntr_nm,
    case cc.inactive_ind
        when 1 then 0 else 1
    end as cc_active_ind,
    get_parent_name.parent_level,
    cc_in_hier.lvl_1_cc_hier_nm,
    cc_in_hier.lvl_2_cc_hier_nm,
    cc_in_hier.lvl_3_cc_hier_nm,
    cc_in_hier.lvl_4_cc_hier_nm,
    cc_in_hier.lvl_5_cc_hier_nm,
    cc_in_hier.lvl_6_cc_hier_nm,
    cc_in_hier.lvl_2_cost_cntr_hierarchy_nm,
    cc_in_hier.lvl_3_cost_cntr_hierarchy_nm,
    cc_in_hier.lvl_4_cost_cntr_hierarchy_nm,
    cc_in_hier.lvl_5_cost_cntr_hierarchy_nm,
    cc_in_hier.lvl_6_cost_cntr_hierarchy_nm,
    get_parent_name.parent_hier_nm as parent_of_cost_center,
	cc_in_hier.cost_center_hier_wid
from
    cc_in_hier
    left join get_parent_name
        on cc_in_hier.cost_center_hier_wid = get_parent_name.parent_hier_wid
    inner join {{ source('workday', 'workday_cost_center') }} as cc
        /* need this for now until the other hierarchies data is avail */
        on cc_in_hier.cost_cntr_id = cc.cost_cntr_id
