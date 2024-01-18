select
    stg_cdw_query.query_key,
    stg_cdw_query.query_date,
    stg_cdw_query.affected_table_name as raw_table_name,
    regexp_replace(
        case
            when query_text like 'select %' then regexp_extract(query_text, '(?<=from )\w+')
            when query_text like 'insert %' then regexp_extract(query_text, '(?<=insert into )\w+' )
            when query_text like 'generate statistics %' then
                regexp_extract(query_text, '(?<=generate statistics on )\w+')
            else affected_table_name
            end,
        '(^pcn_temp_(ins(ert)?_|upd(ate)?_|del(ete)?_|shortcut_to_)?)|'
            || '^pcn_ext_|'
            || 'del_ins_tmp__|'
            || '((_ins(ert)?|_upd(ate)?)?_\d+_\d+$)|'
            || '(?<=\w)_\d{4,}+.*|' -- 1234_1234567890  or  1234567_1_0_1234567890
            || '_soft_delete_\d+_\d+$|'
            || '_(insert|update)_fastrack$|'
            || '(__dbt_tmp|__tmp_bkp|_ins|(soft_)?_del(ete)?|_upd(_keys|_clarity)?)$',
        '' -- remove using empty string
    )::varchar(500) as table_name, -- noqa
    coalesce(
        case when stg_cdw_query.query_text like 'with%' then 'select' end,
        regexp_extract(stg_cdw_query.query_text, '^\w+'),
        stg_cdw_query.main_action_type
     ) as table_action,
    stg_cdw_query.db_name,
    lower(lookup_user_account.user_name) as user_name,
    stg_cdw_query.account_group,
    lookup_user_account.account_subgroup,
    row_number() over(
        partition by stg_cdw_query.query_date, stg_cdw_query.query_text_id
        order by stg_cdw_query.query_start_time
        ) as query_day_seq_num,
    stg_cdw_query.query_start_time,
    stg_cdw_query.query_end_time,
    stg_cdw_query.query_source,
    stg_cdw_query.query_file_path,
    stg_cdw_query.query_source_identifier,
    stg_cdw_query.query_text_id,
    stg_cdw_query.query_text,
    stg_cdw_query.npsid,
    stg_cdw_query.npsinstanceid,
    stg_cdw_query.opid,
    stg_cdw_query.sessionid,
    stg_cdw_query.session_key
from
    {{ ref('stg_cdw_query') }} as stg_cdw_query
    inner join {{ ref('lookup_user_account') }} as lookup_user_account
        on lower(lookup_user_account.user_name) = stg_cdw_query.user_name
where
    lookup_user_account.loader_ind + lookup_user_account.transformer_ind > 0
    and stg_cdw_query.db_name not like '%_dev'
    and coalesce(lookup_user_account.env, '') != 'dev'
