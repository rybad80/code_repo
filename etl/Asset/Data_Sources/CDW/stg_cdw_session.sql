select
    {{
        dbt_utils.surrogate_key([
            'hist_session_prolog.npsid',
            'hist_session_prolog.npsinstanceid',
            'hist_session_prolog.sessionid'
        ])
    }} as session_key,
    hist_session_prolog.sessionuserid as user_id,
    lower(hist_session_prolog.sessionusername) as session_user_name,
    hist_session_prolog.clientip as client_ip_address,
    hist_session_prolog.clienthost as client_host,
    nvl2(lookup_host_client_address.client_host, 1, 0) as platform_host_ind,

    case
        when lower(hist_session_prolog.sessionusername) = 'admin' then 'admin'
        when lower(lookup_host_client_address.client_group) = 'marx' then 'user'
        when hist_session_prolog.clienthost = '[unknown]' and stg_cdw_user.worker_ind = 1 then 'user'
        when (
            (lookup_host_client_address.client_host is not null and stg_cdw_user.worker_id is not null)
            or lower(lookup_host_client_address.client_group) in ('qlik view', 'rstudio connect')
         ) then 'platform run'
        when length(hist_session_prolog.clienthost) in (21, 22) then 'user'
        when stg_cdw_user.service_account_ind = 1 then 'service account'
        when lookup_host_client_address.client_host is not null then 'server'
        else 'other'
        end as query_origin,

     case
        when lower(lookup_host_client_address.client_group) = 'marx' then 'marx'
        when hist_session_prolog.clienthost  = '[unknown]' then 'pc on-site'
        when lookup_host_client_address.client_host is not null then 'server'
        when length(hist_session_prolog.clienthost) in (21, 22) then 'pc remote'
        else 'other'
        end as client_location,
--
    case
        when lookup_host_client_address.client_host is not null
        then coalesce(
            lower(lookup_host_client_address.client_group),
            'server'
        )
        when length(hist_session_prolog.clienthost) in (21, 22) then 'user'
        else 'other'
        end as client_group,

    case
        when lookup_host_client_address.client_host is not null
        then coalesce(
            lower(lookup_host_client_address.client_subgroup),
            lower(lookup_host_client_address.client_group),
            'server'
        )
        when length(hist_session_prolog.clienthost) in (21, 22) then 'user'
        else 'other'
        end as client_subgroup,

    stg_cdw_user.account_group as account_group, -- will show jenkins instead of anayst credentials

    case
        when stg_cdw_user.worker_ind is not null then worker_ind
        when lower(lookup_host_client_address.client_group) = 'marx' then 1
        when lookup_host_client_address.client_host is not null then 0
        when length(hist_session_prolog.clienthost) in (21, 22) then 1
        else 0
        end as worker_ind,
    stg_cdw_user.dna_ind,
    stg_cdw_user.service_account_ind,
    stg_cdw_user.other_account_ind,
    lower(hist_session_prolog.dbname) as db_name,
    date(hist_session_prolog.connecttime + (hist_session_prolog.tzoffset || ' minutes')::interval) as session_date,
    hist_session_prolog.connecttime + (hist_session_prolog.tzoffset || ' minutes')::interval as session_start_time,
    hist_session_epilog.endtime + (hist_session_epilog.tzoffset || ' minutes')::interval as session_end_time,
    extract(epoch from hist_session_epilog.endtime - hist_session_prolog.connecttime)
    / 60.0 as session_duration_mins,
    case hist_session_prolog.clienttype --https://www.ibm.com/docs/en/psfa/7.2.1?topic=tables-hist-session-prolog-n
        when 0 then 'None'
        when 1 then 'nzsql'
        when 2 then 'ODBC'
        when 3 then 'JDBC'
        when 4 then 'nzload / nzunload'
        when 5 then 'Client manager'
        when 6 then 'nzbackup / nzrestore'
        when 7 then 'nzreclaim'
        when 8 then 'Unused'
        when 9 then 'Internal'
        when 10 then 'OLE DB'
        end as connection_type,
    hist_session_prolog.npsid,
    hist_session_prolog.npsinstanceid,
    hist_session_prolog.sessionid
from
    {{ source('histdb', 'hist_session_prolog') }} as hist_session_prolog
    left join {{ source('histdb', 'hist_session_epilog') }} as hist_session_epilog
        on hist_session_epilog.npsid = hist_session_prolog.npsid
        and hist_session_epilog.npsinstanceid = hist_session_prolog.npsinstanceid
        and hist_session_epilog.sessionid = hist_session_prolog.sessionid
    left join {{ ref('lookup_host_client_address') }} as lookup_host_client_address
        on lookup_host_client_address.client_host = hist_session_prolog.clienthost
    inner join {{ ref('stg_cdw_user') }} as stg_cdw_user
        on stg_cdw_user.user_id = hist_session_prolog.sessionuserid
where
    hist_session_prolog.connecttime  + (hist_session_prolog.tzoffset || ' minutes')::interval
        >= current_date - interval('{{var("dev_num_days_to_include")}} days')
