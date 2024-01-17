select
    stg_cdw_query.query_key,
    stg_cdw_query.query_date,
    stg_cdw_query.client_host,
    stg_cdw_query.client_group,
    stg_cdw_query.client_subgroup,
    stg_cdw_query.user_name,
    lookup_user_account.account_owner,
    lookup_user_account.account_group,
    coalesce(lookup_user_account.account_subgroup, lookup_user_account.account_group) as account_subgroup,
    stg_cdw_query.query_start_time,
    stg_cdw_query.query_end_time,
    stg_cdw_query.runtime_secs,
    stg_cdw_query.runtime_mins,
    stg_cdw_query.cost_log10,
    stg_cdw_query.memory_used_log10,
    stg_cdw_query.n_plans,
    stg_cdw_query.n_snippets,
    stg_cdw_query.n_rows,
    stg_cdw_query.query_impact_score,
    stg_cdw_query.hour_start,
    stg_cdw_query.start_before_7am_ind,
    stg_cdw_query.long_query_length_ind,
    stg_cdw_query.query_text_length,
    stg_cdw_query.db_name,
    stg_cdw_query.query_source,
    stg_cdw_query.query_file_path,
    stg_cdw_query.query_source_identifier,
    stg_cdw_query.query_text_id,
    stg_cdw_query.query_text,
    stg_cdw_query.npsid,
    stg_cdw_query.npsinstanceid,
    stg_cdw_query.opid,
    stg_cdw_query.sessionid
from
    {{ ref('stg_cdw_query') }} as stg_cdw_query
    inner join {{ ref('lookup_user_account') }} as lookup_user_account
        on lower(lookup_user_account.user_name) = stg_cdw_query.user_name
where
    stg_cdw_query.client_location = 'server'
    and lookup_user_account.env != 'dev'
    and lookup_user_account.consumer_ind = 1
