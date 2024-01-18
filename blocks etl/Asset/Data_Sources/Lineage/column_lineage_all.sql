select
    mu_column_lineage_key as column_lineage_key,
    -- targets
    'column' as target_type,
    target_column_node,
    source_column_node,
    'mu_column_lineage' as lineage_source,
    target_column_node_key,
    source_column_node_key
from
    {{ ref('mu_column_lineage') }}

union all

select
    asset_column_lineage_key as column_lineage_key,
    -- targets
    'asset' as target_type,
    target_column_node,
    source_column_node,
    'dw_asset_column_lineage' as lineage_source,
    target_column_node_key,
    source_column_node_key
from
    {{ ref('dw_asset_column_lineage') }}
where
    -- exclude informatica (loader), blocks (transformer), etc
    query_purpose = 'consumer'
    -- but keeps automarts
    or query_source = 'analytics/automarts'
