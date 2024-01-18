select distinct
    program_hierarchy_reference_wid as program_hierarchy_wid,
    program_hierarchy_reference_program_hierarchy_id as program_hierarchy_id,
    program_hierarchy_data_program_hierarchy_name as program_hierarchy_name,
    contains_program_reference_wid as program_wid,
    program.program_id,
    cast({{
        dbt_utils.surrogate_key([
            'program_hierarchy_wid',
            'program_hierarchy_id',
            'program_hierarchy_name',
            'program_wid',
            'program_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_program_hierarchies')}} as get_program_hierarchies
left join
    {{ref('program')}} as program
        on get_program_hierarchies.contains_program_reference_wid = program.program_wid
where
    1 = 1
