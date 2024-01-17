select
    {{ dbt_utils.surrogate_key([ 'id2']) }} as qliksense_log_entry_key, --is this unique, need more data
    hostname as process_host,
    app_id as qliksense_app_id,
    app_title as application_title,
    active_user_id as user_name,
    case
        when active_user_id in ('hup') then 1
        when regexp_like(
            active_user_id,
            '_|'        -- underscore       : sa_scheduler
            || '[a-z]0|'  -- letters -> 0     : kphed041
            || '\d[a-z]|' -- digit -> letters : m11nwt41
            || '^svc'     -- starts svc       : svcqliknprinting
        ) then 1
        else 0
    end as service_account_ind,
    regexp_extract(session_start_date, '^(\d{8})')::date as usage_date,
    timezone(
        to_timestamp(substr(session_start_date, 1, 19), 'YYYYMMDD"T"HH24MISS"."MS'),
        'UTC',
        'America/New_York'
    )::timestamp as session_start,
    session_start + ((session_duration * 24 * 60 * 60)::int || ' seconds')::interval as session_end,
    round(session_duration * 24 * 60, 1) as session_duration_mins,
    (session_duration * 24 * 60 * 60)::int as session_duration_secs,
    -- processing
    round(cpu_spent, 1) as cpu_time_secs,
    bytes_received,
    bytes_sent,
    calls as n_calls,
    selections as n_selections,
    -- other session info
    exit_reason,
    secure_protocol,
    -- logger
    logger,
    log_level,
    engine_thread,
    -- server
    exe_version,
    service_user,
    timezone(
        to_timestamp(substr(server_started, 1, 19), 'YYYYMMDD"T"HH24MISS"."MS'),
        'UTC',
        'America/New_York'
    )::timestamp as server_start,
    -- ids
    process_id,
    proxy_package_id,
    sequence_number,
    proxysession_id as session_login_id,
    session_id as session_app_id,
    event_id,
    {{ dbt_utils.surrogate_key([ "'qlik sense'", 'app_id']) }} as asset_inventory_key
from
    {{ source('qliksense_log_ods', 'qliksense_log_session_engine') }} as qliksense_log_session_engine
