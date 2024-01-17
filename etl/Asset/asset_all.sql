with
    automarts as (
        select
            asset_inventory_key,
            'automart' as asset_type,
            owner_user_name,
            case
                when table_name like 's_%' or table_name like 'stg_%' then 'stage'
                when table_name like 'fact_%' then 'fact'
                else 'other'
                end as asset_subtype,
            table_name as asset_id,
            table_name as asset_name,
            'github' as location_type,
            repo_name as location_name,
            'https://github.research.chop.edu/CQI/' || repo_name as url,
            null as created_date,
            last_usage_date,
            n_users_90_day::int as n_users_90_day,
            users_90_day,
            n_accessed_90_day,
            null as active_dna_collaborator_ind,
            null as active_dna_collaborator
        from
            {{ ref('asset_automarts') }}
    ),
    business_objects as (
        select
            asset_inventory_key,
            'business objects' as asset_type,
            null as owner_user_name,
            object_type as asset_subtype,
            bo_content_key::varchar(200) as asset_id,
            object_name as asset_name,
            'prod' as location_type,
            'prod' as location_name,
            null as url,
            null as created_date,
            last_usage_date,
            n_users_90_day,
            users_90_day,
            n_user_runs_90_day as n_accessed_90_day,
            null as active_dna_collaborator_ind,
            null as active_dna_collaborator
        from
            {{ ref('asset_business_objects') }}
    ),
    qliksense as (
        select
            asset_inventory_key,
            'qlik sense' as asset_type,
            owner_user_name,
            case when chqa_governed_ind = 1 then 'governed' else 'ungoverned' end as asset_subtype,
            qliksense_app_id::varchar(200) as asset_id,
            application_title as asset_name,
            'stream' as location_type,
            stream_name as location_name,
            url,
            created_date,
            last_usage_date,
            n_users_90_day,
            user_names_90_day,
            n_accessed_90_day,
            0 as active_dna_collaborator_ind,
            null as active_dna_collaborator
        from
            {{ ref('asset_qliksense') }}
        where
            stream_name is not null -- remove unpublished
    ),
    qlikview as (
        select
            asset_inventory_key,
            'qlik view' as asset_type,
            null as owner_user_name,
            'prod' as asset_subtype,
            qlikview_app_key::varchar(200) as asset_id,
            application_title as asset_name,
            'prod' as location_type,
            'prod' as location_name,
            null as url,
            null as created_date,
            last_usage_date,
            n_users_90_day,
            users_90_day,
            n_users_90_day as n_accessed_90_day,
            0 as active_dna_collaborator_ind,
            null as active_dna_collaborator
        from
            {{ ref('asset_qlikview') }}
    ),
    posit_connect as (
        select
            asset_inventory_key,
            'posit connect' as asset_type,
            owner_user_name,
            content_type as asset_subtype,
            content_guid::varchar(200) as asset_id,
            content_name as asset_name,
            case when git_backed_ind = 1 then 'github' else 'server' end as location_type,
            case when git_backed_ind = 1 then github_repository end as location_name,
            app_url as url,
            created_date,
            last_usage_date,
            n_users_90_day,
            users_90_day,
            n_accessed_90_day,
            active_dna_collaborator_ind,
            active_dna_collaborator
        from
            {{ ref('asset_rstudio_connect') }}
    ),
    all_assets as (
        select * from automarts
        union all
        select * from business_objects
        union all
        select * from qliksense
        union all
        select * from qlikview
        union all
        select * from posit_connect
    )
select
    all_assets.asset_inventory_key,
    all_assets.asset_type,
    all_assets.owner_user_name,
    all_assets.asset_subtype,
    all_assets.asset_id,
    all_assets.asset_name,
    all_assets.location_type,
    all_assets.location_name,
    all_assets.url,
    all_assets.created_date,
    all_assets.last_usage_date,
    all_assets.n_users_90_day,
    all_assets.users_90_day,
    all_assets.n_accessed_90_day,
    all_assets.active_dna_collaborator_ind,
    all_assets.active_dna_collaborator,
    worker.worker_id,
    worker.preferred_reporting_name as owner_name,
    asset_query_metrics.last_run_date,
    asset_query_metrics.n_query,
    asset_query_metrics.total_cost_log10,
    asset_query_metrics.total_memory_used_log10,
    asset_query_metrics.total_snippets,
    asset_query_runtime_percentile.total_runtime_10_pctl,
    asset_query_runtime_percentile.total_runtime_25_pctl,
    asset_query_runtime_percentile.total_runtime_50_pctl,
    asset_query_runtime_percentile.total_runtime_75_pctl,
    asset_query_runtime_percentile.total_runtime_90_pctl,
    round(asset_query_runtime_percentile.total_runtime_10_pctl * 0.011, 2) as projected_snowflake_credits,
    asset_query_metrics.total_query_text_length,
    asset_query_metrics.total_rows,
    asset_query_metrics.total_query_impact_score,
    case when asset_query_metrics.total_query_impact_score is not null then 1 else 0 end as has_metrics_ind,
    case when asset_query_metrics.total_cost_log10 >= 6 then 1 else 0 end as high_cost_ind,
    case when asset_query_metrics.total_memory_used_log10 >= 5 then 1 else 0 end as high_memory_ind,
    case when asset_query_metrics.total_snippets > 20 then 1 else 0 end as large_snippets_ind,
    case when asset_query_metrics.total_runtime_mins > 3 then 1 else 0 end as long_runtime_ind,
    case when asset_query_metrics.total_query_text_length > 6000 then 1 else 0 end as long_text_ind,
    case when asset_query_metrics.total_rows = 0 then 1 else 0 end as no_rows_ind,
    case when asset_query_metrics.total_query_impact_score > 10 then 1 else 0 end as high_query_impact_ind,
    case when last_usage_date > current_date - 90 then 1 else 0 end as past_90_day_ind,
    case when (users_90_day != owner_user_name and n_users_90_day > 0) then 1 else 0 end as other_users_90_day_ind,
    asset_query_metrics.uses_tdl_ind,
    asset_query_metrics.uses_ods_ind,
    asset_query_metrics.uses_cdw_ind,
    asset_query_metrics.uses_ocqi_ind,
    asset_query_metrics.uses_other_db_ind,
    worker.manager_name,
    worker.active_ind as active_employee_ind,
    case when analyst.active_ind = 1 and analyst.expat_ind = 0 then 1 else 0 end as active_dna_ind,
    case when analyst.worker_id is not null then 1 else 0 end as dna_ind,
    coalesce(analyst.expat_ind, 0) as expat_ind
from
    all_assets
    left join {{ ref('asset_query_metrics') }} as asset_query_metrics
        on all_assets.asset_inventory_key = asset_query_metrics.asset_inventory_key
    left join {{ ref('asset_query_runtime_percentile') }} as asset_query_runtime_percentile
        on all_assets.asset_inventory_key = asset_query_runtime_percentile.asset_inventory_key
    left join {{ ref('worker') }} as worker
        on worker.ad_login = all_assets.owner_user_name
    left join {{ ref('stg_asset_dna_analyst') }} as analyst
        on analyst.worker_id = worker.worker_id
