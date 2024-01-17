select
    stg_cdw_scheduled_query.query_key,
    stg_cdw_scheduled_query.query_date,
    stg_cdw_scheduled_query.table_name, -- noqa
    stg_cdw_scheduled_query.raw_table_name,
    stg_cdw_scheduled_query.table_action,
    stg_cdw_scheduled_query.db_name,
    stg_cdw_scheduled_query.user_name,
    stg_cdw_scheduled_query.account_group,
    stg_cdw_scheduled_query.account_subgroup,
    stg_cdw_scheduled_query.query_start_time,
    stg_cdw_scheduled_query.query_end_time,
    to_char(
        stg_cdw_scheduled_query.query_start_time - (
            mod(minute(stg_cdw_scheduled_query.query_start_time), 5) || ' min'
        )::interval,
         'HH24:MI'
    ) as five_min_timestamp,
    stg_cdw_query.cost_log10,
    stg_cdw_query.memory_used_log10,
    stg_cdw_query.n_plans,
    stg_cdw_query.n_snippets,
    stg_cdw_query.runtime_secs as query_runtime_secs,
    stg_cdw_query.n_rows,
    stg_cdw_query.query_impact_score,
    extract(hour from stg_cdw_scheduled_query.query_end_time) as hour_end,
    case
        when extract(hour from stg_cdw_scheduled_query.query_end_time) < 7 then 1 else 0
    end as end_before_7am_ind,
    stg_cdw_query.query_text_length,
    stg_cdw_query.long_query_length_ind,
    stg_cdw_query.query_source,
    stg_cdw_query.query_file_path,
    stg_cdw_query.query_source_identifier,
    stg_cdw_scheduled_query.query_text_id,
    stg_cdw_scheduled_query.query_text,
    stg_cdw_scheduled_query.npsid,
    stg_cdw_scheduled_query.npsinstanceid,
    stg_cdw_scheduled_query.opid,
    stg_cdw_scheduled_query.sessionid,
    stg_cdw_scheduled_query.session_key
from
    {{ ref('stg_cdw_scheduled_query') }} as stg_cdw_scheduled_query
    inner join {{ ref('stg_cdw_query') }} as stg_cdw_query using (query_key)
where
    stg_cdw_scheduled_query.query_day_seq_num = 1 -- first successful run of the day
