with critical_tables_history as (
    select
        readyornot_critical_tables_snapshot.db_name,
        readyornot_critical_tables_snapshot.table_name,
        coalesce(lookup_readyornot_critical_models_history.first_critical_run_date,
            readyornot_critical_tables_snapshot.dbt_valid_from) as dbt_valid_from,
        coalesce(readyornot_critical_tables_snapshot.dbt_valid_to, current_date) as dbt_valid_to
    from
        {{ ref ('readyornot_critical_tables_snapshot') }} as readyornot_critical_tables_snapshot
        left join {{ ref('lookup_readyornot_critical_models_history') }} as lookup_readyornot_critical_models_history
            on lookup_readyornot_critical_models_history.table_name = readyornot_critical_tables_snapshot.table_name
                and lookup_readyornot_critical_models_history.db_name = readyornot_critical_tables_snapshot.db_name
                and lookup_readyornot_critical_models_history.first_critical_run_date > readyornot_critical_tables_snapshot.dbt_valid_from
),
informatica_history as ( -- legacy tables, stage clarity
    select
        readyornot_infa_sessions_run_history.workflow_run_id,
        readyornot_infa_sessions_run_history.start_time::date as run_date,
        readyornot_infa_sessions_run_history.workflow_name, -- DAG: multiple within fasttrack vs clarity
        readyornot_infa_sessions_run_history.session_name,  -- Task: fasttrack.visit vs clarity.visit
        readyornot_infa_sessions_run_history.repo_name,
        'informatica' as account_group, -- matches other cdw_* tables
        null as db_name,
        readyornot_infa_sessions_run_history.target_name as table_name,   -- table name
        nvl2(readyornot_critical_workflows_snapshot.workflow_name, 1,0) as critical_ind,
        min(readyornot_infa_sessions_run_history.start_time)::datetime as start_date,
        min(readyornot_infa_sessions_run_history.end_time)::datetime as end_date,
        extract(epoch from end_date - start_date) as runtime_secs,
        1 as run_seq_num, -- needed for airflow CTEs in order to union
        sum(readyornot_infa_sessions_run_history.applied_rows) as n_rows
    from
        {{ source('readyornot_ods', 'readyornot_infa_sessions_run_history') }}
            as readyornot_infa_sessions_run_history
        left join {{ ref('readyornot_critical_workflows_snapshot') }} as readyornot_critical_workflows_snapshot
            on readyornot_critical_workflows_snapshot.workflow_name = readyornot_infa_sessions_run_history.workflow_name
            and readyornot_infa_sessions_run_history.start_time::date between
                readyornot_critical_workflows_snapshot.dbt_valid_from
                    and coalesce(readyornot_critical_workflows_snapshot.dbt_valid_to, current_date)
    where
        hour(start_time::datetime) < 13 -- before 1pm to avoid runs at night
   group by
        readyornot_infa_sessions_run_history.workflow_run_id,
        readyornot_infa_sessions_run_history.start_time::date,
        readyornot_infa_sessions_run_history.workflow_name,
        readyornot_infa_sessions_run_history.session_name,
        readyornot_infa_sessions_run_history.repo_name,
        readyornot_infa_sessions_run_history.target_name,
        readyornot_critical_workflows_snapshot.workflow_name
),
informatica_today as ( -- legacy tables, stage clarity
    select
        1 as workflow_run_id,
        readyornot_infa_sessions_today.start_time::date as run_date,
        readyornot_infa_sessions_today.workflow_name, -- DAG: multiple within fasttrack vs clarity
        readyornot_infa_sessions_today.session_name,  -- Task: fasttrack.visit vs clarity.visit
        readyornot_infa_sessions_today.repo_name,
        'informatica' as account_group,
        null as db_name,
        readyornot_infa_sessions_today.target_name as table_name,   -- table name
        nvl2(readyornot_critical_workflows_snapshot.workflow_name, 1,0) as critical_ind,
        min(readyornot_infa_sessions_today.start_time)::datetime as start_date,
        max(readyornot_infa_sessions_today.end_time)::datetime as end_date,
        extract(epoch from end_date - start_date) as runtime_secs,
        1 as run_seq_num, -- needed for airflow CTEs in order to union
        sum(readyornot_infa_sessions_today.applied_rows) as n_rows
    from
        {{ source('readyornot_ods', 'readyornot_infa_sessions_today') }} as readyornot_infa_sessions_today
        left join {{ ref('readyornot_critical_workflows_snapshot') }} as readyornot_critical_workflows_snapshot
            on readyornot_critical_workflows_snapshot.workflow_name = readyornot_infa_sessions_today.workflow_name
            and readyornot_infa_sessions_today.start_time::date between
                readyornot_critical_workflows_snapshot.dbt_valid_from
                    and coalesce(readyornot_critical_workflows_snapshot.dbt_valid_to, current_date)
    where
        hour(start_time::datetime) < 13
   group by
        readyornot_infa_sessions_today.start_time::date,
        readyornot_infa_sessions_today.workflow_name,
        readyornot_infa_sessions_today.session_name,
        readyornot_infa_sessions_today.repo_name,
        readyornot_infa_sessions_today.target_name,
        readyornot_critical_workflows_snapshot.workflow_name
),
airflow_history as ( -- ods, tdl & automarts
    select
        1 as workflow_run_id,
        readyornot_table_status_history.started_timestamp::date as run_date,
        readyornot_table_status_history.owner_name as workflow_name, -- need
        readyornot_table_status_history.owner_name as session_name,
        readyornot_table_status_history.owner_name as repo_name, -- need
        'airflow' as account_group,
        readyornot_table_status_history.db_name,
        readyornot_table_status_history.table_name,
        nvl2(critical_tables_history.table_name, 1, 0) as critical_ind,
        readyornot_table_status_history.started_timestamp::datetime as start_date,
        readyornot_table_status_history.completed_timestamp::datetime as end_date,
        extract(epoch from end_date - start_date) as runtime_secs,
        row_number() over(
            partition by
                run_date, readyornot_table_status_history.db_name, readyornot_table_status_history.table_name
            order by readyornot_table_status_history.started_timestamp
        ) as run_seq_num,
        null as n_rows
    from
        {{ source('readyornot_ods', 'readyornot_table_status_history') }} as readyornot_table_status_history
        left join critical_tables_history
            on critical_tables_history.table_name = lower(readyornot_table_status_history.table_name)
            and critical_tables_history.db_name = lower(readyornot_table_status_history.db_name)
            and readyornot_table_status_history.started_timestamp::date between
                critical_tables_history.dbt_valid_from and critical_tables_history.dbt_valid_to
    where
        readyornot_table_status_history.completed_by in ('AIRFLOW')
        and readyornot_table_status_history.is_completed = 1
        and hour(readyornot_table_status_history.started_timestamp::datetime) < 13
),
airflow_today as ( -- ods, tdl & automarts
    select
        1 as workflow_run_id,
        readyornot_table_status.started_timestamp::date as run_date,
        readyornot_table_status.owner_name as workflow_name, -- need
        readyornot_table_status.owner_name as session_name,
        readyornot_table_status.owner_name as repo_name, -- need
        'airflow' as account_group,
        readyornot_table_status.db_name,
        readyornot_table_status.table_name,
        nvl2(critical_tables_history.table_name, 1, 0) as critical_ind,
        readyornot_table_status.started_timestamp::datetime as start_date,
        readyornot_table_status.completed_timestamp::datetime as end_date,
        extract(epoch from end_date - start_date) as runtime_secs,
        row_number() over(
            partition by run_date, readyornot_table_status.db_name, readyornot_table_status.table_name
            order by readyornot_table_status.started_timestamp
        ) as run_seq_num,
        null as n_rows
    from
        {{ source('readyornot_ods', 'readyornot_table_status') }} as readyornot_table_status
        left join critical_tables_history
            on critical_tables_history.table_name = lower(readyornot_table_status.table_name)
            and critical_tables_history.db_name = lower(readyornot_table_status.db_name)
            and readyornot_table_status.started_timestamp::date between
                critical_tables_history.dbt_valid_from and critical_tables_history.dbt_valid_to
        where
            readyornot_table_status.completed_by in ('AIRFLOW')
            and readyornot_table_status.is_completed = 1
            and hour(readyornot_table_status.started_timestamp::datetime) < 13
),
combined as (
    select * from informatica_history
    union all
    select * from informatica_today
    union all
    select * from airflow_history where run_seq_num = 1
    union all
    select * from airflow_today where run_seq_num = 1
)
select
    {{
        dbt_utils.surrogate_key([
            'run_date',
            'workflow_run_id',
            'workflow_name',
            'session_name',
            'repo_name',
            'db_name',
            'table_name'
        ])
    }} as workflow_history_key,
    run_date,
    row_number() over(partition by run_date order by start_date) as run_order,
    lower(workflow_name) as workflow_name,
    lower(session_name) as session_name,
    lower(repo_name) as repo_name,
    account_group,
    case -- need from API, still missing several
        when account_group = 'airflow' then lower(db_name)
        -- by repo
        when lower(repo_name) like '%%_ods%_' then 'cdw_ods'
        when lower(repo_name) in ('cdw_extract', 'workday') then 'cdwprd'
        when lower(repo_name) = 'clarity_extract' then 'cdw_ods'
        when lower(repo_name) = 'clarity_load' then '???'
        when lower(repo_name) like '%\_ods' then 'cdw_ods'
        -- by workflow
        when lower(workflow_name) like 'wf%\_ods\_%' then 'cdw_ods'
        when lower(workflow_name) like '%\_data\_lake' then 'cdw_ods'
        when lower(workflow_name) like '%stage' then 'cdw_stg'
        when lower(workflow_name) like '%zc' and lower(table_name) like 's\_%' then 'cdw_stg'
        when lower(workflow_name) like '%\_cdw\_%' and lower(session_name) like 's\_stg%' then 'cdw_stg'
        when lower(workflow_name) like '%\_cdw\_customer%' then 'cdw_customer'
        when lower(workflow_name) like '%\_cdw' then 'cdwprd'
        when lower(workflow_name) like '%\_cdw\_%' then 'cdwprd'
        when lower(workflow_name) like '%zc' then 'cdwprd'
        when lower(workflow_name) like 'wf\_syngo%' then 'cdwprd'
        when lower(workflow_name) like '%\_core\_%' then 'cdwprd'
        -- by session
        when lower(session_name) like 's\_cdw\_load%' then 'cdwprd'
        when lower(session_name) like 's\_ods\_load%' then 'cdw_ods'
        end as db_name,
    lower(table_name) as table_name,
    critical_ind,
    start_date,
    end_date,
    runtime_secs,
    n_rows,
    {{
        dbt_utils.surrogate_key([
            'workflow_name',
            'session_name',
            'repo_name',
            'db_name',
            'table_name'
        ])
    }} as workflow_table_key,
    row_number() over(partition by run_date, workflow_table_key order by start_date) as run_order_per_workflow_per_day_history,
    case when run_order_per_workflow_per_day_history = 1 then 1 else 0 end as first_run_of_day_ind,
    row_number() over(partition by workflow_table_key order by start_date) as run_order_per_workflow_history,
    case when run_order_per_workflow_history = 1 then 1 else 0 end as first_run_ind
from
    combined
