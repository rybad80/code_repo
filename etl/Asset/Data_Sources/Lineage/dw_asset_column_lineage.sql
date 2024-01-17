select distinct
    {{
        dbt_utils.surrogate_key([
            'dw_asset_query_xref.asset_inventory_key',
            'lower(hist_column_access.dbname)',
            'lower(hist_column_access.tablename)',
            'lower(hist_column_access.columnname)'
        ])
    }} as asset_column_lineage_key,
    dw_asset_query_xref.query_date as last_run_date,
    dw_asset_query_xref.query_purpose,
    -- target
    dw_asset_query_xref.asset_type,
    dw_asset_query_xref.query_source,
    dw_asset_query_xref.query_file_path,
    dw_asset_query_xref.query_source_identifier,
    -- sources
    case -- need these for joins
        when hist_column_access.dbname = 'CHOP_ANALYTICS' then 'blocks'
        when hist_column_access.dbname like 'CDW_ODS%' then 'data_lake'
        when hist_column_access.dbname like 'CDW___' then 'cdw' --cdwprd and cdwods
        when hist_column_access.dbname like 'OCQI_%' then 'automarts'
        else hist_column_access.dbname
        end as source_db,
    lower(hist_column_access.tablename) as source_table,
    lower(hist_column_access.columnname) as source_column,
    case
        when dw_asset_query_xref.asset_type not in ('automart', 'blocks') then
        'asset'
            || ' / ' || dw_asset_query_xref.asset_type
            || ' / ' || dw_asset_query_xref.query_file_path
            || ' / ' || dw_asset_query_xref.query_source_identifier
            end as target_column_node,
        'netezza'
            || ' / ' || source_db
            || ' / ' || source_table
            || ' / ' || source_column
            as source_column_node,
    -- TDL keys
    --dw_asset_query_xref.query_key,
    {{
        dbt_utils.surrogate_key([
            'lower(hist_column_access.dbname)',
            'lower(hist_column_access.schemaname)',
            'lower(hist_column_access.tablename)',
            'lower(hist_column_access.columnname)'
        ])
    }} as column_key, -- matches usage tables
    {{
        dbt_utils.surrogate_key([
            'lower(hist_column_access.dbname)',
            'lower(hist_column_access.schemaname)',
            'lower(hist_column_access.tablename)'
        ])
    }} as table_key, -- matches usage tables
    {{
        dbt_utils.surrogate_key([
            'dw_asset_query_xref.asset_inventory_key',
            'lower(hist_column_access.dbname)',
            'lower(hist_column_access.tablename)'
        ])
    }} as asset_table_lineage_key,
    {{ dbt_utils.surrogate_key(['target_column_node']) }} as target_column_node_key,
    {{ dbt_utils.surrogate_key(['source_column_node']) }} as source_column_node_key,
    dw_asset_query_xref.asset_inventory_key
    --,
    -- link to histdb
    --dw_asset_query_xref.session_key,
    --dw_asset_query_xref.npsid,
    --dw_asset_query_xref.npsinstanceid,
    --dw_asset_query_xref.opid
from
    {{ ref('dw_asset_query_xref') }} as dw_asset_query_xref
    inner join {{ source('histdb', 'hist_column_access') }} as hist_column_access using(npsid, npsinstanceid, opid)
where
    dw_asset_query_xref.latest_run_ind = 1
    -- TODO: allow for source to be missing and create indicator
    and dw_asset_query_xref.query_source_identifier != ''
