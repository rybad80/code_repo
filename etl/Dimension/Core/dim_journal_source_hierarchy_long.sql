{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['journal_source_hier_key', 'journal_source', 'journal_source_name', 'top_level', 'journal_source_hierarchy_level', 'hier_level_num', 'update_date', 'hash_value', 'integration_id', 'update_by'],
    meta = {
        'critical': true
    }
  )
}}
with journal_source_hier_details
as (
select
    lookup_journal_source_hierarchy.journal_source,
    lookup_journal_source_hierarchy.journal_source_name,
    lookup_journal_source_hierarchy.level_1_hierarchy
from
    {{ref('lookup_journal_source_hierarchy')}} as lookup_journal_source_hierarchy
),
journal_source_hier_long
as (
select
    journal_source,
    journal_source_name,
    'All Journal Sources' as top_level,
    'All Journal Sources' as journal_source_hierarchy_level,
    1 as hier_level,
    'MANUAL' as create_by,
    'MANUAL' as upd_by
from
    journal_source_hier_details
--
union all
--
select
    journal_source,
    journal_source_name,
    'All Journal Sources' as top_level,
    level_1_hierarchy as journal_source_hierarchy_level,
    2 as hier_level,
    'MANUAL' as create_by,
    'MANUAL' as upd_by
from
    journal_source_hier_details
),
journal_source_hier
as (
select
    {{
        dbt_utils.surrogate_key([            
            'journal_source',
            'top_level',
            'hier_level'
        ])
    }} as journal_source_hier_key, 
    journal_source,
    journal_source_name,
    top_level,
    journal_source_hierarchy_level,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'journal_source',
            'journal_source_name',
            'top_level',
            'journal_source_hierarchy_level',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || journal_source || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    journal_source_hier_long
where 1 = 1
--
union all
--
select
    0, 
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP, 
    'DEFAULT'
)
select
    journal_source_hier_key, 
    journal_source,
    journal_source_name,
    top_level,
    journal_source_hierarchy_level,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    journal_source_hier
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = journal_source_hier.integration_id)
{%- endif %}
