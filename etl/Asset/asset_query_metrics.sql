with
query_overflow as (
    select
        dw_asset_query_xref.query_key,
        sum(length(hist_query_overflow.querytext)) as query_text_length
    from
        {{ ref('dw_asset_query_xref') }} as dw_asset_query_xref -- already uses latest_run_ind = 1
        inner join {{ source('histdb', 'hist_query_overflow') }} as hist_query_overflow
            using (npsid, npsinstanceid, opid)
    where
        dw_asset_query_xref.latest_run_ind = 1
    group by
        dw_asset_query_xref.query_key
),

db_usage as (
    select
        dw_asset_query_xref.asset_inventory_key,
        max(case when hist_column_access.dbname = 'CHOP_ANALYTICS' then 1 else 0 end) as uses_tdl_ind,
        max(case when hist_column_access.dbname like 'CDW_ODS%' then 1 else 0 end) as uses_ods_ind,
        max(case when hist_column_access.dbname like 'CDW___' then 1 else 0 end) as uses_cdw_ind, --cdwprd, cdwuat
        max(case when hist_column_access.dbname like 'OCQI_%' then 1 else 0 end) as uses_ocqi_ind,
        case
            when uses_tdl_ind + uses_ods_ind + uses_cdw_ind + uses_ocqi_ind = 0 then 1
            else 0
            end as uses_other_db_ind
    from
        {{ ref('dw_asset_query_xref') }} as dw_asset_query_xref
        inner join {{ source('histdb', 'hist_column_access') }} as hist_column_access
            using (npsid, npsinstanceid, opid)
    where
        dw_asset_query_xref.latest_run_ind = 1
    group by
        dw_asset_query_xref.asset_inventory_key
),

query_stats as (
    select
        dw_asset_query_xref.asset_inventory_key,
        dw_asset_query_xref.asset_type,
        dw_asset_query_xref.query_source_identifier,
        -- stats
        dw_asset_query_xref.query_date as last_run_date,
        count(distinct dw_asset_query_xref.query_key) as n_query,
        round(
            log(
                -- if sum of cost goes to high, the query will break, this maxes out at 10^20
                min(1e20, sum(pow(10, dw_asset_query_xref.cost_log10)))
            ),
            1
        ) as total_cost_log10,
        round(
            log(
                sum(pow(10, dw_asset_query_xref.memory_used_log10))
            ),
            1
        ) as total_memory_used_log10,
        sum(dw_asset_query_xref.n_snippets) as total_snippets,
        round(sum(dw_asset_query_xref.runtime_mins), 1) as total_runtime_mins,
        sum(
            dw_asset_query_xref.query_text_length
            + coalesce(query_overflow.query_text_length, 0)
        ) as total_query_text_length,
        round(sum(dw_asset_query_xref.query_impact_score), 1) as total_query_impact_score,
        sum(dw_asset_query_xref.n_rows) as total_rows
    from
        {{ ref('dw_asset_query_xref') }} as dw_asset_query_xref
        left join query_overflow
            on query_overflow.query_key = dw_asset_query_xref.query_key
    where
        dw_asset_query_xref.latest_run_ind = 1 -- use latest run only
    group by
        dw_asset_query_xref.asset_inventory_key,
        dw_asset_query_xref.asset_type,
        dw_asset_query_xref.query_source_identifier,
        dw_asset_query_xref.query_date
)

select
    -- asset
    query_stats.asset_inventory_key,
    query_stats.asset_type,
    query_stats.query_source_identifier,
    query_stats.last_run_date,
    -- DB usage
    db_usage.uses_tdl_ind,
    db_usage.uses_ods_ind,
    db_usage.uses_cdw_ind,
    db_usage.uses_ocqi_ind,
    db_usage.uses_other_db_ind,
    -- query metrics
    query_stats.n_query,
    query_stats.total_query_impact_score,
    query_stats.total_cost_log10,
    query_stats.total_memory_used_log10,
    query_stats.total_snippets,
    query_stats.total_runtime_mins,
    query_stats.total_query_text_length,
    query_stats.total_rows,
    -- metric indicators
    case when query_stats.total_cost_log10 >= 6 then 1 else 0 end as high_cost_ind,
    case when query_stats.total_memory_used_log10 > 5 then 1 else 0 end as high_memory_ind,
    case when query_stats.total_snippets > 20 then 1 else 0 end as large_snippets_ind,
    case when query_stats.total_runtime_mins > 3 then 1 else 0 end as long_runtime_ind,
    case when query_stats.total_query_text_length > 6000 then 1 else 0 end as long_text_ind,
    case when query_stats.total_rows = 0 then 1 else 0 end as no_rows_ind
from
    query_stats
    inner join db_usage
        on db_usage.asset_inventory_key = query_stats.asset_inventory_key
