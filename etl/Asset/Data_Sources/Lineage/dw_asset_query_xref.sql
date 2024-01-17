select
    stg_cdw_query.query_key,
    stg_cdw_query.query_date,
    case
        -- align these names with mu for column / table lineage
        when stg_cdw_query.client_group = 'posit connect' then 'posit connect'
        when stg_cdw_query.query_source = 'analytics/automarts' then 'automart'
        when stg_cdw_query.query_source = 'analytics/chop-data-blocks' then 'blocks'
        when stg_cdw_query.query_source = 'analytics/qlik-sense-documents' then 'qlik sense'
        when stg_cdw_query.query_source = 'analytics/qlikview-documents' then 'qlik view'
        else stg_cdw_query.query_source
        end as asset_type,
    case
        when stg_cdw_query.query_origin = 'platform run' then 'consumer'
        when lookup_user_account.loader_ind = 1 then 'loader'
        when lookup_user_account.transformer_ind = 1 then 'transformer'
        when lookup_user_account.consumer_ind = 1 then 'consumer'
        when lookup_user_account.service_account_ind = 1 then 'service'
        when lookup_user_account.other_account_ind = 1 then 'other'
        else 'unknown'
        end as query_purpose,
    stg_cdw_query.query_source,
    case
        when stg_cdw_query.client_group = 'qlik sense' then qliksense_qsr_streams.name
        when stg_cdw_query.query_file_path != '' then stg_cdw_query.query_file_path
        end as query_file_path,
    stg_cdw_query.query_source_identifier,
    case
        when stg_cdw_query.client_group = 'qlik sense' then qliksense_qsr_apps.name
        when stg_cdw_query.client_group = 'rstudio connect' then asset_rstudio_connect.content_name
        else stg_cdw_query.query_source_identifier
        end as query_source_name,
    case
        when stg_cdw_query.query_source = 'analytics/automarts'
            then 'https://github.research.chop.edu/CQI/'
            || query_file_path
            || '/tree/HEAD/Code/SQL'
        when stg_cdw_query.query_source = 'analytics/chop-data-blocks'
            then 'https://github.research.chop.edu/analytics/chop-data-blocks/tree/HEAD/etl/'
            || query_file_path
        when stg_cdw_query.client_group = 'qlik sense'
            then 'https://github.research.chop.edu/analytics/qlik-sense-documents/tree/HEAD/'
            || qliksense_qsr_apps.name
            || stg_cdw_query.query_source_identifier
        when stg_cdw_query.client_group = 'qlik view' then
            'https://github.research.chop.edu/analytics/qlikview-documents/search?q=filename%3A%22'
            || regexp_replace(regexp_replace(stg_cdw_query.query_source_identifier, ' ', '+'), '.qvw$', '')
            ||'%22'
        end as code_url,
    case when stg_cdw_query.query_source_identifier = '' then 1 else 0 end as missing_identifier_ind,
    stg_cdw_query.query_origin,
    stg_cdw_query.query_status,
    stg_cdw_query.query_start_time,
    stg_cdw_query.query_end_time,
    stg_cdw_query.client_group,
--    client_subgroup,
    stg_cdw_query.client_host,
    stg_cdw_query.user_name,
    stg_cdw_query.n_rows,
    stg_cdw_query.query_impact_score,
    stg_cdw_query.cost_log10,
    stg_cdw_query.memory_used_log10,
    stg_cdw_query.n_snippets,
    stg_cdw_query.n_plans,
    stg_cdw_query.runtime_mins,
    stg_cdw_query.query_text_length,
    substr(stg_cdw_query.query_text, 1, 500)::varchar(500) as query_text_500, -- keep record size lower
    case
        when (
            case
                when query_date = max(query_date) over(
                    partition by asset_type, stg_cdw_query.query_source_identifier
                ) then rank() over(
                    partition by stg_cdw_query.query_source_identifier, query_text_id
                    order by stg_cdw_query.query_start_time desc
                )
                end
        ) = 1 then 1
        else 0
    end as latest_run_ind,
    -- link to stg_cdw_query and histdb
    stg_cdw_query.session_key,
    stg_cdw_query.npsid,
    stg_cdw_query.npsinstanceid,
    stg_cdw_query.opid,
    {{
        dbt_utils.surrogate_key([
            'asset_type',
            'stg_cdw_query.query_source_identifier'
        ])
    }} as asset_inventory_key
from
    {{ ref('stg_cdw_query') }} as stg_cdw_query
    left join {{ ref('lookup_user_account') }} as lookup_user_account
        on lower(lookup_user_account.user_name) = stg_cdw_query.user_name
    left join {{ source('qliksense_ods', 'qliksense_qsr_apps') }} as qliksense_qsr_apps
        on qliksense_qsr_apps.id = stg_cdw_query.query_source_identifier
        and stg_cdw_query.client_group = 'qlik sense'
    left join {{ source('qliksense_ods', 'qliksense_qsr_streams') }} as qliksense_qsr_streams
      on qliksense_qsr_streams.id = qliksense_qsr_apps.stream_id
    left join {{ ref('asset_rstudio_connect') }} as asset_rstudio_connect
        on stg_cdw_query.query_source_identifier = asset_rstudio_connect.content_guid
        and stg_cdw_query.client_group = 'rstudio connect'
where
    stg_cdw_query.query_origin in ('platform run', 'service account')
    and stg_cdw_query.has_query_stats_ind = 1 -- should this filter be applied?
    and coalesce(lookup_user_account.loader_ind, 0) != 1 -- for now, if we get informatica, we can remove this
    -- remove local dev and CI jobs, we can infer the links & impact from the tables running during ETL
    and stg_cdw_query.db_name != 'chop_analytics_dev'
    and stg_cdw_query.user_name != 'svc_blocks_ar_dev'
