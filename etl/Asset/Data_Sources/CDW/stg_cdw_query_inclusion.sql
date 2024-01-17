with
query_action as (
    select
        {{
            dbt_utils.surrogate_key([
                'hist_query_prolog.npsid',
                'hist_query_prolog.npsinstanceid',
                'hist_query_prolog.opid'
            ])
        }} as query_key,
        max(
            case
                when hist_table_access.usage in (2, 4, 8, 16, 32, 64, 512) then lower(hist_table_access.tablename)
            end
        ) as affected_table_name,
        max(
            case when hist_table_access.usage in (2, 4, 8, 16, 32, 64, 512) then 1 else 0 end
        ) as affected_table_ind,
        -- see https://www.ibm.com/docs/en/psfa/7.2.1?topic=tables-hist-table-access-n
        -- often will see bitwise logic for usage: (usage & (1 << 6)) >> 6
        max(case when hist_table_access.usage = 1 then 1 else 0 end) as select_ind,
        max(case when hist_table_access.usage = 2 then 1 else 0 end) as insert_ind,
        max(case when hist_table_access.usage = 4 then 1 else 0 end) as delete_ind,
        max(case when hist_table_access.usage = 8 then 1 else 0 end) as update_ind,
        max(case when hist_table_access.usage = 16 then 1 else 0 end) as truncate_ind,
        max(case when hist_table_access.usage = 32 then 1 else 0 end) as drop_ind,
        max(case when hist_table_access.usage = 64 then 1 else 0 end) as create_ind,
        max(case when hist_table_access.usage = 128 then 1 else 0 end) as stats_ind,
        max(case when hist_table_access.usage = 256 then 1 else 0 end) as lock_ind,
        max(case when hist_table_access.usage = 512 then 1 else 0 end) as alter_ind,
        case
            -- prioritize these actions
            when insert_ind = 1 then 'insert'
            when delete_ind = 1 then 'delete'
            when update_ind = 1 then 'update'
            when truncate_ind = 1 then 'truncate'
            when drop_ind = 1 then 'drop'
            when create_ind = 1 then 'create'
            when stats_ind = 1 then 'stats'
            when lock_ind = 1 then 'lock'
            when alter_ind = 1 then 'alter'
            -- then select last
            when select_ind = 1 then 'select'
            else 'other'
            end as main_action_type,
        -- flag queries to ignore as all steps point at 'definition_schema'
        case
            when sum(case when hist_table_access.schemaid = 4 then 1 else 0 end) = count(*) then 1
            else 0
            end as ignore_ind,
        hist_query_prolog.npsid,
        hist_query_prolog.npsinstanceid,
        hist_query_prolog.opid
    from
        {{ source('histdb', 'hist_table_access') }} as hist_table_access
        inner join {{ source('histdb', 'hist_query_prolog')}} as hist_query_prolog
            on hist_query_prolog.npsid = hist_table_access.npsid
            and hist_query_prolog.npsinstanceid = hist_table_access.npsinstanceid
            and hist_query_prolog.sessionid = hist_table_access.sessionid
            and hist_query_prolog.opid = hist_table_access.opid
    where
        hist_query_prolog.submittime + (hist_query_prolog.tzoffset || ' minutes')::interval
            >= current_date - interval('{{var("dev_num_days_to_include")}} days')
    group by
        hist_query_prolog.npsid,
        hist_query_prolog.npsinstanceid,
        hist_query_prolog.opid
)

select
    {{
        dbt_utils.surrogate_key([
            'hist_query_prolog.npsid',
            'hist_query_prolog.npsinstanceid',
            'hist_query_prolog.opid'
        ])
    }} as query_key,
    date(hist_query_prolog.submittime + (hist_query_prolog.tzoffset || ' minutes')::interval) as query_date,
    hist_query_prolog.submittime as submit_time_utc,
    nvl2(query_action.query_key, 1, 0) as has_query_action_ind,
    -- update action if query mentions create
    case
        when lower(hist_query_prolog.querytext) like 'call %' then 'call'
        when lower(hist_query_prolog.querytext) like '%create table %'
            or lower(hist_query_prolog.querytext) like '%create view %'
            or lower(hist_query_prolog.querytext) like '%create external table %'
            then 'create'
        when lower(hist_query_prolog.querytext) like 'groom table %' then 'groom'
        when lower(hist_query_prolog.querytext) like '%insert into %' then 'insert'
        when lower(hist_query_prolog.querytext) like 'lock table %' then 'lock'
        when query_action.affected_table_ind is null then 'other'
        else query_action.main_action_type
    end as main_action_type,
    -- update indicator
    case
        when lower(hist_query_prolog.querytext) like 'call %'
            or lower(hist_query_prolog.querytext) like '%create table %'
            or lower(hist_query_prolog.querytext) like '%create view %'
            or lower(hist_query_prolog.querytext) like '%create external table %'
            or lower(hist_query_prolog.querytext) like '%groom table %'
            or lower(hist_query_prolog.querytext) like '%insert into %'
            or lower(hist_query_prolog.querytext) like 'lock table %'
            then 1
        else coalesce(query_action.affected_table_ind, 0)
    end as affected_table_ind,
    --used upstream
    replace(lower(substring(hist_query_prolog.querytext, 1, 500)), chr(10), ' ') as action_search_text,
    query_action.affected_table_name,
    -- keys & ids
    {{
        dbt_utils.surrogate_key([
            'hist_query_prolog.npsid',
            'hist_query_prolog.npsinstanceid',
            'hist_query_prolog.sessionid'
        ])
    }} as session_key,
    hist_query_prolog.npsid,
    hist_query_prolog.npsinstanceid,
    hist_query_prolog.opid,
    hist_query_prolog.sessionid
from
    {{ source('histdb', 'hist_query_prolog') }} as hist_query_prolog
    left join query_action using (npsid, npsinstanceid, opid)
where
    hist_query_prolog.submittime + (hist_query_prolog.tzoffset || ' minutes')::interval
        >= current_date - interval('{{var("dev_num_days_to_include")}} days')
    -- remove queries flagged to ignore or missing from query_action table
    and coalesce(query_action.ignore_ind, 0) = 0
