select
    stg_cdw_query_inclusion.query_key,
    stg_cdw_query_inclusion.query_date,
    case when hist_query_epilog.status = 0 then 1 else 0 end as successful_ind,
    case hist_query_epilog.status
        when 0 then 'success'
        when -1 then 'session aborted'
        when -2 then 'canceled by user'
        when -3 then 'failed parsing'
        when -4 then 'failed rewrite'
        when -5 then 'failed planning'
        when -6 then 'failed execution'
        when -7 then 'reserved'
        when -8 then 'failed ACL'
        when -9 then 'failed generic'
        end as query_status,
    stg_cdw_session.query_origin,
    stg_cdw_session.client_location,
    stg_cdw_session.client_group,
    stg_cdw_session.client_subgroup,
    stg_cdw_session.account_group, -- will show jenkins instead of anayst credentials
    hist_query_prolog.userid as user_id,
    lower(hist_query_prolog.username) as user_name,
    stg_cdw_session.connection_type,
    stg_cdw_session.db_name,
    hist_query_prolog.submittime + (hist_query_prolog.tzoffset || ' minutes')::interval as query_start_time,
    hist_query_epilog.finishtime +  (hist_query_epilog.tzoffset || ' minutes')::interval as query_end_time,
    extract(epoch from query_end_time - query_start_time) as runtime_secs,
    extract(epoch from query_end_time - query_start_time) / 60.0 as runtime_mins,
    hist_query_epilog.resultrows as n_rows,
    stg_cdw_session.worker_ind,
    stg_cdw_session.dna_ind,
    stg_cdw_session.service_account_ind,
    stg_cdw_session.other_account_ind as other_account_ind,
    extract(hour from query_start_time) as hour_start,
    case when extract(hour from query_start_time) < 7 then 1 else 0 end as start_before_7am_ind,
    length(hist_query_prolog.querytext) as query_text_length,
    case when length(hist_query_prolog.querytext) >= 8000 then 1 else 0 end as long_query_length_ind,
    stg_cdw_query_inclusion.has_query_action_ind,
    stg_cdw_query_inclusion.main_action_type,
    stg_cdw_query_inclusion.affected_table_ind,
    coalesce(
        {#
         find table that is created, must work for these scenarios:
            create external table "pcn_ext_workday_cost_center_hierarchy_xref_20447428_0_0_1674479190" sameas "p
            create external table '/infa/informatica/10.2/server/infa_shared/temp/pipepcn_piperdr10879208_0_0__1
            create external table '/opt/airflow/data_files/cdw-extracts-cardiac-ph/phmedications' using (remotes
            create table chop_analytics.admin.stg_bh_notes_smart_texts__dbt_tmp as ( select note_visit_info.note
            create table chop_analytics_dev.admin.stg_perfusion_meds__dbt_tmp as ( with stg_perfusion_meds as (
            create table fact_uro_pro_responses as with cohort as ( select * from /*insert cohort*/ s_uro_pro_qu
            create table ocqi_uat..stg_cohort_endo_visit_last_md_vis as select pat_key, endo_vis_dt from ocqi_ua
            create table ocqi_uat.admin.fact_surgery_rampup_unsched__dbt_tmp as ( with timeframe as ( select or_
            create view chop_analytics.admin.fact_journal_lines_posted__dbt_tmp as ( select * from chop_analytic
         #}
        stg_cdw_query_inclusion.affected_table_name,
        case
            when stg_cdw_query_inclusion.main_action_type != 'create' then null
            when regexp_like(stg_cdw_query_inclusion.action_search_text, 'create external table [''"]')
                then regexp_extract(
                    stg_cdw_query_inclusion.action_search_text,
                    '(?<=create external table .)[^''"]+'
                )
            else regexp_replace(
                stg_cdw_query_inclusion.action_search_text,
                '(.*create (table|view) )(\w+\.\w*\.)?(\w+)( as .*)',
                '\4'
            )
            end
    ) as affected_table_name,
    nvl2(stg_cdw_query_stats.query_key, 1, 0) as has_query_stats_ind,
    stg_cdw_query_stats.cost_log10,
    stg_cdw_query_stats.memory_used_log10,
    stg_cdw_query_stats.n_snippets,
    stg_cdw_query_stats.n_plans,
    (
        (coalesce(stg_cdw_query_stats.cost_log10, 0) / 5.5) --300K
        + (coalesce(stg_cdw_query_stats.n_snippets, 0) / 21) * 3 -- ex. 75 / 21 = 3.6 * 3 = 10.8 points added
        + (coalesce(stg_cdw_query_stats.memory_used_log10, 0) / 5) -- 100K
        + (coalesce(query_text_length, 0) / 6000)
        + (case
            when coalesce(hist_query_epilog.resultrows, 1) = 0
                and stg_cdw_query_inclusion.main_action_type in ('select', 'insert', 'update')
                then 3
                else 0
            end
        )
    ) as query_impact_score,
    case
        -- add in repo names so URLs can be created before we had query source identifers in our apps
        when stg_cdw_session.client_group = 'SAP' then stg_cdw_session.client_subgroup
        when stg_cdw_session.client_group = 'qlik sense' then 'analytics/qlik-sense-documents'
        when stg_cdw_session.client_group = 'qlik view' then 'analytics/qlikview-documents'
        when hist_query_prolog.client_workstation_name = 'automart' then 'analytics/automarts'
        when hist_query_prolog.client_workstation_name != '' then hist_query_prolog.client_workstation_name
        when stg_cdw_session.client_group is not null then stg_cdw_session.client_group
        else ''
        end as query_source,
    hist_query_prolog.client_application_name as query_file_path,
    case
        when stg_cdw_session.client_group = 'qlik view' -- remove qlik view extension
            then regexp_replace(hist_query_prolog.client_accounting_string, '(?i)\.qvw$', '')
        else hist_query_prolog.client_accounting_string
        end as query_source_identifier,
    translate( -- remove carriage return and quotes
        lower(hist_query_prolog.querytext),
        chr(10) || '"',
        ''
    ) as query_text,
    hist_query_prolog.checksum as query_text_id,
    hist_query_prolog.npsid,
    hist_query_prolog.npsinstanceid,
    hist_query_prolog.sessionid,
    hist_query_prolog.opid,
    hist_query_prolog.logentryid,
    stg_cdw_session.client_ip_address,
    stg_cdw_session.client_host,
    stg_cdw_session.session_key
from
    -- only relevant queries
    {{ ref('stg_cdw_query_inclusion') }} as stg_cdw_query_inclusion
    -- about queries results, cost is lower when left join are first
    left join {{ source('histdb', 'hist_query_epilog') }} as hist_query_epilog
        on hist_query_epilog.npsid = stg_cdw_query_inclusion.npsid
        and hist_query_epilog.npsinstanceid = stg_cdw_query_inclusion.npsinstanceid
        and hist_query_epilog.opid = stg_cdw_query_inclusion.opid
    -- about session
    left join {{ ref('stg_cdw_session') }} as stg_cdw_session
        on stg_cdw_session.session_key = stg_cdw_query_inclusion.session_key
    -- info about stats
    left join {{ ref('stg_cdw_query_stats') }} as stg_cdw_query_stats
        on stg_cdw_query_stats.query_key = stg_cdw_query_inclusion.query_key
    -- about queries sent
    inner join {{ source('histdb', 'hist_query_prolog') }} as hist_query_prolog
        on hist_query_prolog.npsid = stg_cdw_query_inclusion.npsid
        and hist_query_prolog.npsinstanceid = stg_cdw_query_inclusion.npsinstanceid
        and hist_query_prolog.opid = stg_cdw_query_inclusion.opid
where
    -- remove queries that have no outcome
    stg_cdw_query_inclusion.has_query_action_ind = 1
    or stg_cdw_query_stats.query_key is not null
    -- or query did not complete
    or hist_query_epilog.status is null
