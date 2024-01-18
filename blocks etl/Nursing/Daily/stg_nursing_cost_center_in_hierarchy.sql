{{ config(meta = {
    'critical': true
}) }}

-- a. get hierarchy placement for cost center:  stg_nursing_Cost_Center_in_Hierarchy 
select
    xref.cost_cntr_key,
    xref.cost_cntr_nm,
    case xref.cost_cntr_hierarchy_key
        when lvls.lvl_1_cost_cntr_hierarchy_key then 1
        when lvls.lvl_2_cost_cntr_hierarchy_key then 2
        when lvls.lvl_3_cost_cntr_hierarchy_key then 3
        when lvls.lvl_4_cost_cntr_hierarchy_key then 4
        when lvls.lvl_5_cost_cntr_hierarchy_key then 5
        when lvls.lvl_6_cost_cntr_hierarchy_key then 6
    end as parent_level,
	lvls.lvl_1_cost_cntr_hierarchy_nm as lvl_1_cc_hier_nm,
    case
        when lvl_2_cost_cntr_hierarchy_key = 0 then ''
        else lvls.lvl_2_cost_cntr_hierarchy_nm
    end as lvl_2_cc_hier_nm,
    case
        when lvl_3_cost_cntr_hierarchy_key = 0 then ''
        else lvls.lvl_3_cost_cntr_hierarchy_nm
    end as lvl_3_cc_hier_nm,
    case
        when lvl_4_cost_cntr_hierarchy_key = 0 then ''
        else lvls.lvl_4_cost_cntr_hierarchy_nm
    end as lvl_4_cc_hier_nm,
    case
        when lvl_5_cost_cntr_hierarchy_key = 0 then ''
        else lvls.lvl_5_cost_cntr_hierarchy_nm
    end as lvl_5_cc_hier_nm,
    case
        when lvl_6_cost_cntr_hierarchy_key = 0 then ''
        else  lvls.lvl_6_cost_cntr_hierarchy_nm
    end as lvl_6_cc_hier_nm,
    lvls.lvl_2_cost_cntr_hierarchy_nm,
    lvls.lvl_3_cost_cntr_hierarchy_nm,
    lvls.lvl_4_cost_cntr_hierarchy_nm,
    lvls.lvl_5_cost_cntr_hierarchy_nm,
    lvls.lvl_6_cost_cntr_hierarchy_nm,
    hier.cost_cntr_hierarchy_nm as parent_cost_cntr_hierarchy_nm
from
    {{ source('workday', 'workday_cost_center_hierarchy_xref') }} as xref
inner join {{ source('workday', 'workday_cost_center_hierarchy_levels') }} as lvls
    on xref.cost_cntr_hierarchy_key = lvls.cost_cntr_hierarchy_key
inner join {{ source('workday', 'workday_cost_center_hierarchy') }} as hier
    on xref.cost_cntr_hierarchy_key = hier.cost_cntr_hierarchy_key
