{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = [ 'ledger_account_id', 'ledger_account_name', 'top_level', 'ledger_account_hierarchy_level', 'hier_level_num', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
with ledger_account_hier_details
as (
select
    ledger_account.ledger_account_id,
    ledger_account.ledger_account_name,
    ledger_account.create_by,
    ledger_account.upd_by,
    max(ledger_account_summary_levels.level_1_ledger_account_summary_name) as ledger_account_hierarchy_level1_name,
    max(ledger_account_summary_levels.level_2_ledger_account_summary_name) as ledger_account_hierarchy_level2_name,
    max(ledger_account_summary_levels.level_3_ledger_account_summary_name) as ledger_account_hierarchy_level3_name,
    max(ledger_account_summary_levels.level_4_ledger_account_summary_name) as ledger_account_hierarchy_level4_name,
    max(ledger_account_summary_levels.level_5_ledger_account_summary_name) as ledger_account_hierarchy_level5_name,
    max(ledger_account_summary_levels.level_6_ledger_account_summary_name) as ledger_account_hierarchy_level6_name
from
    {{source('workday_ods', 'ledger_account_ledger_account_summary')}} as ledger_account_ledger_account_summary,
    {{source('workday_ods', 'ledger_account_summary_levels')}} as ledger_account_summary_levels,
    {{source('workday_ods', 'ledger_account')}} as ledger_account
where
    1 = 1
    and ledger_account.ledger_account_id = ledger_account_ledger_account_summary.ledger_account_id 
    and ledger_account_ledger_account_summary.ledger_account_summary_id
    = ledger_account_summary_levels.ledger_account_summary_id
group by
    ledger_account.ledger_account_id,
    ledger_account.ledger_account_name,
    ledger_account.create_by,
    ledger_account.upd_by
),
ledger_account_hier_long
as (
select
    ledger_account_id,
    ledger_account_name,
    ledger_account_hierarchy_level1_name as top_level,
    ledger_account_hierarchy_level1_name as ledger_account_hierarchy_level,
    1 as hier_level,
    create_by,
    upd_by
from
    ledger_account_hier_details
--    
union all
--
select
    ledger_account_id,
    ledger_account_name,
    ledger_account_hierarchy_level1_name as top_level,
    ledger_account_hierarchy_level2_name as ledger_account_hierarchy_level,
    2 as hier_level,
    create_by,
    upd_by
from
    ledger_account_hier_details
where
    ledger_account_hierarchy_level2_name is not null
--    
union all
--
select
    ledger_account_id,
    ledger_account_name,
    ledger_account_hierarchy_level1_name as top_level,
    ledger_account_hierarchy_level3_name as ledger_account_hierarchy_level,
    3 as hier_level,
    create_by,
    upd_by
from
    ledger_account_hier_details
where
    ledger_account_hierarchy_level3_name is not null
--    
union all
--
select
    ledger_account_id,
    ledger_account_name,
    ledger_account_hierarchy_level1_name as top_level,
    ledger_account_hierarchy_level4_name as ledger_account_hierarchy_level,
    4 as hier_level,
    create_by,
    upd_by
from
    ledger_account_hier_details
where
    ledger_account_hierarchy_level4_name is not null
--    
union all
--
select
    ledger_account_id,
    ledger_account_name,
    ledger_account_hierarchy_level1_name as top_level,
    ledger_account_hierarchy_level5_name as ledger_account_hierarchy_level,
    5 as hier_level,
    create_by,
    upd_by
from
    ledger_account_hier_details
where
    ledger_account_hierarchy_level5_name is not null
--    
union all
--
select
    ledger_account_id,
    ledger_account_name,
    ledger_account_hierarchy_level1_name as top_level,
    ledger_account_hierarchy_level6_name as ledger_account_hierarchy_level,
    6 as hier_level,
    create_by,
    upd_by
from
    ledger_account_hier_details
where
    ledger_account_hierarchy_level6_name is not null    
),
ledger_account_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'ledger_account_id',
            'top_level',
            'hier_level'
        ])
    }} as ledger_account_hier_key, 
    ledger_account_id,
    ledger_account_name,
    top_level,
    ledger_account_hierarchy_level,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'ledger_account_id',
            'ledger_account_name',
            'top_level',
            'ledger_account_hierarchy_level',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || ledger_account_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    ledger_account_hier_long
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
    ledger_account_hier_key, 
    ledger_account_id,
    ledger_account_name,
    top_level,
    ledger_account_hierarchy_level,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    ledger_account_hier
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = ledger_account_hier.integration_id)
{%- endif %}
