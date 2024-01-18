select distinct
    asset_table_lineage_key,
    last_run_date,
    query_purpose,
    -- target
    asset_type,
    query_source,
    query_file_path,
    query_source_identifier,
    -- sources
    source_db,
    source_table,
    case
        when asset_type not in ('automart', 'blocks') then
            'asset / ' || asset_type || ' / ' || query_file_path || ' / ' || query_source_identifier
        else
            'netezza / '
            || case asset_type when 'automart' then 'automarts' else 'blocks' end
            || ' / ' || query_source_identifier
            end as target_table_node,
    'netezza'
        || ' / ' || source_db
        || ' / ' || source_table
        as source_table_node,
    -- TDL keys
    table_key, -- matches usage tables
    {{ dbt_utils.surrogate_key(['target_table_node']) }} as target_table_node_key,
    {{ dbt_utils.surrogate_key(['source_table_node']) }} as source_table_node_key,
    asset_inventory_key
from
    {{ ref('dw_asset_column_lineage') }}
