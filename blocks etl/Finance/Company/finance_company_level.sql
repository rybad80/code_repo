select
    company.company_id,
    company.company_name,
    company.company_code,
    company.inactive_ind,
    company_hierarchy.company_hierarchy_name as parent_hierarchy_name,
    company_hierarchy.company_hierarchy_id  as parent_hierarchy_id,
    case company_hierarchy.company_hierarchy_id
        when company_hierarchy_levels.company_hierarchy_level1_id then 1
        when company_hierarchy_levels.company_hierarchy_level2_id then 2
        when company_hierarchy_levels.company_hierarchy_level3_id then 3
    end as parent_level,
    company_hierarchy_levels.company_hierarchy_level1_name as level_1_name,
    company_hierarchy_levels.company_hierarchy_level2_name as level_2_name,
    company_hierarchy_levels.company_hierarchy_level3_name as level_3_name,
    company_hierarchy_levels.company_hierarchy_level1_id as company_hierarchy_level_1_id,
    company_hierarchy_levels.company_hierarchy_level2_id as company_hierarchy_level_2_id,
    company_hierarchy_levels.company_hierarchy_level3_id as company_hierarchy_level_3_id,
    company_company_hierarchy.company_wid,
    company_hierarchy_levels.company_hierarchy_level1_wid as company_hierarchy_level_1_wid,
    company_hierarchy_levels.company_hierarchy_level2_wid as company_hierarchy_level_2_wid,
    company_hierarchy_levels.company_hierarchy_level3_wid as company_hierarchy_level_3_wid
from
    {{source('workday_ods', 'company_company_hierarchy')}} as company_company_hierarchy
inner join
    {{source('workday_ods', 'company_hierarchy')}} as company_hierarchy
        on company_company_hierarchy.company_hierarchy_wid  = company_hierarchy.company_hierarchy_wid
inner join
    {{source('workday_ods', 'company')}} as company
        on company_company_hierarchy.company_wid = company.company_wid
inner join
    {{source('workday_ods', 'company_hierarchy_levels')}} as company_hierarchy_levels
        on company_company_hierarchy.company_hierarchy_wid = company_hierarchy_levels.company_hierarchy_wid
