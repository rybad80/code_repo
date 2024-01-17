with get_table_info as (
    select
        get_dba_table_rows.owner,
        get_dba_table_rows.table_name as table_nm,
        cast(coalesce(get_dba_table_rows.num_rows, 0) as bigint) as total_num_rows,
        case
            when dba_tables_strategy.table_name is null then 'N'
            else 'Y'
        end as row_tracking_enabled_yn
    from
        {{ source('clarity_ods', 'dba_tables') }} as get_dba_table_rows
        left join
            {{ source('clarity_ods', 'dba_tables') }} as dba_tables_strategy on
                'csa_' || lower(get_dba_table_rows.table_name) = lower(dba_tables_strategy.table_name)
    where
        lower(get_dba_table_rows.owner) in ('clarity', 'sys', 'cbmi')
),
get_clarity_execution_info as (
    select
        x_cl.table_name,
        case
            when lower(x_cl.exec_name) like '%primary%' then 'primary'
            when lower(x_cl.exec_name) like '%other%' then 'other'
            when lower(x_cl.exec_name) like '%audit%' or lower(x_cl.exec_name) like '%access%' then 'audit'
        end as execution_name,
        x_cl.exec_start_time as last_etl_execution_date,
        row_number() over (
            partition by lower(x_cl.table_name)
            order by x_cl.exec_start_time desc
        ) as instance_rank
    from
        {{ source('clarity_ods', 'x_clarity_extract_info') }} as x_cl
    where
        lower(x_cl.status) in ('success', 'warning')
    group by
        x_cl.table_name,
        x_cl.exec_name,
        x_cl.exec_start_time
)
select
    lower(all_objects.owner) as owner_name,
    lower(all_objects.object_name) as relation_name,
    lower(all_objects.object_type) as relation_type,
    lower(z_t.name) as clarity_specific_relation_type,
    get_clarity_execution_info.execution_name,
    case
        when lower(c_tl.load_frequency) like '%weekly%' then 'weekly'
        when lower(c_tl.load_frequency) like '%monthly%' then 'monthly'
        when lower(c_tl.load_frequency) in ('incremental', 'primary', 'audit')
            or c_tl.load_frequency is null then 'daily'
    end as execution_frequency,
    get_table_info.total_num_rows,
    get_table_info.row_tracking_enabled_yn,
    c_tl.deprecated_yn,
    case
        when clarity_specific_relation_type = 'derived table'
            and c_tl_2.dertbl_supports_rut_yn is null then 'N'
        else c_tl_2.dertbl_supports_rut_yn
    end as derived_table_supports_row_tracking_yn,
    all_objects.created as creation_date,
    get_clarity_execution_info.last_etl_execution_date
from
    {{ source('clarity_ods', 'all_objects') }} as all_objects
    left join {{ source('clarity_ods', 'clarity_tbl') }} as c_tl
        on lower(all_objects.object_name) = lower(c_tl.table_name)
            and lower(table_id) like 'c%' /* it has a c-record */
    left join {{ source('clarity_ods', 'clarity_tbl_2') }} as c_tl_2
        on c_tl.table_id = c_tl_2.table_id
    left join {{ source('clarity_ods', 'zc_table_type') }} as z_t
        on z_t.table_type_c = c_tl_2.table_type_c
    left join get_table_info on lower(get_table_info.table_nm) = lower(all_objects.object_name)
        and lower(all_objects.owner) = lower(get_table_info.owner)
    left join get_clarity_execution_info
        on lower(get_table_info.table_nm) = lower(get_clarity_execution_info.table_name)
        and get_clarity_execution_info.execution_name is not null
        and get_clarity_execution_info.instance_rank = 1
where
    lower(all_objects.owner) in ('sys', 'clarity', 'cbmi')
    and lower(all_objects.object_type) in ('table', 'view')
