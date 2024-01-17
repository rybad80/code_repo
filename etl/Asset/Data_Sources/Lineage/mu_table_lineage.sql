select distinct
    {{
        dbt_utils.surrogate_key([
            "'mu'",
            'target_table_id',
            'source_table_id'
        ])
    }} as mu_table_lineage_key,
    -- targets
    target_system,
    target_db,
    target_table,
    -- sources
    source_system,
    source_db,
    source_table,
    target_system || ' / ' || target_db || ' / ' || target_table as target_table_node,
    source_system || ' / ' || source_db || ' / ' || source_table as source_table_node,
    -- ids
    target_system_id,
    target_db_id,
    target_table_id,
    source_system_id,
    source_db_id,
    source_table_id,
    source_column_id,
    {{ dbt_utils.surrogate_key(['target_table_node']) }} as target_table_node_key,
    {{ dbt_utils.surrogate_key(['source_table_node']) }} as source_table_node_key
from
    {{ ref('mu_column_lineage') }}
