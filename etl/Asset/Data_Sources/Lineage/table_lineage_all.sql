select
    mu_table_lineage_key as table_lineage_key,
    -- targets
    'table' as target_type,
    target_table_node,
    source_table_node,
    'mu_table_lineage' as lineage_source,
    target_table_node_key,
    source_table_node_key
from
    {{ ref('mu_table_lineage') }}

union all

select
    asset_table_lineage_key as table_lineage_key,
    -- targets
    'asset' as target_type,
    target_table_node,
    source_table_node,
    'dw_asset_table_lineage' as lineage_source,
    target_table_node_key,
    source_table_node_key
from
    {{ ref('dw_asset_table_lineage') }}
where
    -- exclude informatica (loader), blocks (transformer), etc
    query_purpose = 'consumer'
    -- but keeps automarts
    or query_source = 'analytics/automarts'
