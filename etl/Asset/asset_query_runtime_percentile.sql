with
query_stats as (
    select
        dw_asset_query_xref.query_date,
        dw_asset_query_xref.asset_inventory_key,
        dw_asset_query_xref.asset_type,
        dw_asset_query_xref.query_source_name,
        dw_asset_query_xref.query_source_identifier,
        sum(dw_asset_query_xref.runtime_mins) as total_runtime_mins
    from
        {{ ref('dw_asset_query_xref') }} as dw_asset_query_xref
    where
        dw_asset_query_xref.query_date >= current_date - 30
        and dw_asset_query_xref.missing_identifier_ind = 0
    group by
        dw_asset_query_xref.query_date,
        dw_asset_query_xref.asset_inventory_key,
        dw_asset_query_xref.asset_type,
        dw_asset_query_xref.query_source_name,
        dw_asset_query_xref.query_source_identifier
)

select
    asset_inventory_key,
    -- noqa: disable=L012, PRS
    percentile_cont(0.10) within group (order by total_runtime_mins) as total_runtime_10_pctl,
    percentile_cont(0.25) within group (order by total_runtime_mins) as total_runtime_25_pctl,
    percentile_cont(0.50) within group (order by total_runtime_mins) as total_runtime_50_pctl,
    percentile_cont(0.75) within group (order by total_runtime_mins) as total_runtime_75_pctl,
    percentile_cont(0.90) within group (order by total_runtime_mins) as total_runtime_90_pctl
    -- noqa: enable=L012, PRS
from
    query_stats
where
    total_runtime_mins > 0.0 -- there are many 'alter', and 'with stats' statements that have 0 runtime
group by
    asset_inventory_key
