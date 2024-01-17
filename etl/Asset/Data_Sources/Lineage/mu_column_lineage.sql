select
    {{
        dbt_utils.surrogate_key([
            "'mu'",
            'column_source.column_id',
            'column_source.source_column_id'
        ])
    }} as mu_column_lineage_key,
    -- targets
    lower(target_system.name) as target_system,
    lower(target_db.name) as target_db,
    lower(target_table.name) as target_table,
    lower(target_column.name) as target_column,
    -- sources
    lower(source_system.name) as source_system,
    lower(source_db.name) as source_db,
    lower(source_table.name) as source_table,
    lower(source_column.name) as source_column,
    target_system || ' / ' || target_db || ' / ' || target_table || ' / ' || target_column as target_column_node,
    source_system || ' / ' || source_db || ' / ' || source_table || ' / ' || source_column as source_column_node,
    -- ids
    target_system.id as target_system_id,
    target_db.id as target_db_id,
    target_table.id as target_table_id,
    target_column.id as target_column_id,
    source_system.id as source_system_id,
    source_db.id as source_db_id,
    source_table.id as source_table_id,
    source_column.id as source_column_id,
    {{ dbt_utils.surrogate_key(['target_column_node']) }} as target_column_node_key,
    {{ dbt_utils.surrogate_key(['source_column_node']) }} as source_column_node_key
from
    {{ source('metadata_universe_ods', 'metadata_universe_column_source') }} as column_source
    -- targets
    inner join {{ source('metadata_universe_ods', 'metadata_universe_entity_column') }} as target_column
        on target_column.id = column_source.column_id
    inner join {{ source('metadata_universe_ods', 'metadata_universe_entity') }} as target_table
        on target_table.id = target_column.entity_id
    inner join {{ source('metadata_universe_ods', 'metadata_universe_data_source') }} as target_db
        on target_db.id = target_table.data_source_id
    inner join {{ source('metadata_universe_ods', 'metadata_universe_data_source_type') }} as target_system
        on target_system.id = target_db.source_type_id
    -- sources
    inner join {{ source('metadata_universe_ods', 'metadata_universe_entity_column') }} as source_column
        on source_column.id = column_source.source_column_id
    inner join {{ source('metadata_universe_ods', 'metadata_universe_entity') }} as source_table
        on source_table.id = source_column.entity_id
    inner join {{ source('metadata_universe_ods', 'metadata_universe_data_source') }} as source_db
        on source_db.id = source_table.data_source_id
    inner join {{ source('metadata_universe_ods', 'metadata_universe_data_source_type') }} as source_system
        on source_system.id = source_db.source_type_id
