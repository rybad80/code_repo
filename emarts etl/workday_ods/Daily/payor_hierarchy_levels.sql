{{
    config(
        materialized = 'incremental',
        unique_key = 'payor_hierarchy_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['payor_hierarchy_wid','payor_hierarchy_id','payor_hierarchy_name','payor_parent_hierarchy_wid','payor_parent_hierarchy_id','payor_toplevel_hierarchy_wid','payor_toplevel_hierarchy_id','payor_hierarchy_level1_wid','payor_hierarchy_level1_id','payor_hierarchy_level1_name','payor_hierarchy_level2_wid','payor_hierarchy_level2_id','payor_hierarchy_level2_name', 'md5', 'upd_dt', 'upd_by']
    )
}}
with Level_1_Hierarchy as (
    select 
        payor_hierarchy.payor_hierarchy_wid as payor_hierarchy_wid,
        payor_hierarchy.payor_hierarchy_id as payor_hierarchy_id,
        payor_hierarchy.payor_hierarchy_name as payor_hierarchy_name,
        payor_hierarchy.organization_type_id,
        payor_hierarchy.organization_subtype_id,
        payor_hierarchy.payor_parent_hierarchy_wid,
        payor_hierarchy.payor_parent_hierarchy_id,
        payor_hierarchy.payor_toplevel_hierarchy_wid,
        payor_hierarchy.payor_toplevel_hierarchy_id,
        payor_hierarchy.payor_hierarchy_wid as payor_hierarchy_level1_wid,
        payor_hierarchy.payor_hierarchy_id as payor_hierarchy_level1_id,
        payor_hierarchy.payor_hierarchy_name as payor_hierarchy_level1_name,
        null as payor_hierarchy_level2_wid,
        null as payor_hierarchy_level2_id,
        null as payor_hierarchy_level2_name
    from
        {{ref('payor_hierarchy')}} as payor_hierarchy
    where
        (payor_hierarchy.organization_type_id = 'ORGANIZATION_TYPE-6-43' and payor_hierarchy.organization_subtype_id = 'Top_Level')
        or (payor_hierarchy.organization_type_id = 'ORGANIZATION_TYPE-6-43'
            and payor_hierarchy.payor_hierarchy_wid = payor_hierarchy.payor_toplevel_hierarchy_wid
        )
),
Level_2_Hierarchy as (
    select
        payor_hierarchy.payor_hierarchy_wid as payor_hierarchy_wid,
        payor_hierarchy.payor_hierarchy_id as payor_hierarchy_id,
        payor_hierarchy.payor_hierarchy_name as payor_hierarchy_name,
        payor_hierarchy.payor_parent_hierarchy_id,
        payor_hierarchy.payor_parent_hierarchy_wid,
        payor_hierarchy.payor_toplevel_hierarchy_id,
        payor_hierarchy.payor_toplevel_hierarchy_wid,
        level_1_hierarchy.payor_hierarchy_level1_wid,
        level_1_hierarchy.payor_hierarchy_level1_id,
        level_1_hierarchy.payor_hierarchy_level1_name,
        payor_hierarchy.payor_hierarchy_wid as payor_hierarchy_level2_wid,
        payor_hierarchy.payor_hierarchy_id as payor_hierarchy_level2_id,
        payor_hierarchy.payor_hierarchy_name as payor_hierarchy_level2_name
    from
        {{ref('payor_hierarchy')}} as payor_hierarchy
    inner join
        Level_1_Hierarchy
            on payor_hierarchy.payor_parent_hierarchy_wid = level_1_hierarchy.payor_hierarchy_level1_wid
),
finaloutput as (
    select
        lvl1.payor_hierarchy_wid,
        lvl1.payor_hierarchy_id,
        lvl1.payor_hierarchy_name,
        lvl1.payor_parent_hierarchy_wid,
        lvl1.payor_parent_hierarchy_id,
        lvl1.payor_toplevel_hierarchy_wid,
        lvl1.payor_toplevel_hierarchy_id,
        lvl1.payor_hierarchy_level1_wid,
        lvl1.payor_hierarchy_level1_id,
        lvl1.payor_hierarchy_level1_name,
        lvl1.payor_hierarchy_level2_wid,
        lvl1.payor_hierarchy_level2_id,
        lvl1.payor_hierarchy_level2_name
    from
        level_1_hierarchy lvl1
    union
    select
        lvl2.payor_hierarchy_wid,
        lvl2.payor_hierarchy_id,
        lvl2.payor_hierarchy_name,
        lvl2.payor_parent_hierarchy_wid,
        lvl2.payor_parent_hierarchy_id,
        lvl2.payor_toplevel_hierarchy_wid,
        lvl2.payor_toplevel_hierarchy_id,
        lvl2.payor_hierarchy_level1_wid,
        lvl2.payor_hierarchy_level1_id,
        lvl2.payor_hierarchy_level1_name,
        lvl2.payor_hierarchy_level2_wid,
        lvl2.payor_hierarchy_level2_id,
        lvl2.payor_hierarchy_level2_name
    from level_2_hierarchy lvl2
)
select distinct
    finaloutput.payor_hierarchy_wid,
    finaloutput.payor_hierarchy_id,
    finaloutput.payor_hierarchy_name,
    finaloutput.payor_parent_hierarchy_wid,
    finaloutput.payor_parent_hierarchy_id,
    finaloutput.payor_toplevel_hierarchy_wid,
    finaloutput.payor_toplevel_hierarchy_id,
    finaloutput.payor_hierarchy_level1_wid,
    finaloutput.payor_hierarchy_level1_id,
    finaloutput.payor_hierarchy_level1_name,
    finaloutput.payor_hierarchy_level2_wid,
    finaloutput.payor_hierarchy_level2_id,
    finaloutput.payor_hierarchy_level2_name,
    cast({{
        dbt_utils.surrogate_key([
            'payor_hierarchy_wid',
            'payor_hierarchy_id',
            'payor_hierarchy_name',
            'payor_parent_hierarchy_wid',
            'payor_parent_hierarchy_id',
            'payor_toplevel_hierarchy_wid',
            'payor_toplevel_hierarchy_id',
            'payor_hierarchy_level1_wid',
            'payor_hierarchy_level1_id',
            'payor_hierarchy_level1_name',
            'payor_hierarchy_level2_wid',
            'payor_hierarchy_level2_id',
            'payor_hierarchy_level2_name'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    finaloutput
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                payor_hierarchy_wid = finaloutput.payor_hierarchy_wid
        )
    {%- endif %}