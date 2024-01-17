{{
  config(
    materialized = 'incremental',
    unique_key = 'project_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['project_id', 'project_name', 'worktag_only_ind', 'inactive_ind', 'include_project_id_in_name', 'start_date', 'end_date', 'billable_ind', 'capital_ind', 'description', 'overall_percent_complete', 'probability_of_success', 'external_project_reference_name', 'problem_statement', 'objective', 'in_scope', 'out_of_scope', 'measures_of_success', 'project_overview', 'estimated_budget', 'estimated_revenue', 'realized_revenue', 'project_group', 'project_state', 'project_status', 'company_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
  )
}}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= source('workday_ods', 'workday_project'), except=['upd_dt']) %}
with project
as (
select
    {{
        dbt_utils.surrogate_key([
            'project_wid'
        ])
    }} as project_key,
    project_wid,
    project_id,
    project_name,
    worktag_only_ind,
    inactive_ind,
    include_project_id_in_name,
    start_date,
    end_date,
    billable_ind,
    capital_ind,
    description,
    overall_percent_complete,
    probability_of_success,
    external_project_reference_name,
    problem_statement,
    objective,
    in_scope,
    out_of_scope,
    measures_of_success,
    project_overview,
    estimated_budget,
    estimated_revenue,
    realized_revenue,
    project_group,
    project_state,
    project_status,
    company_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    'WORKDAY' || '~' || project_id as integration_id,
    current_timestamp as create_date,
    'WORKDAY' as create_by,
    current_timestamp as update_date,
    'WORKDAY' as update_by
from
    {{source('workday_ods', 'workday_project')}}
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    null,
    null,
    null,
    current_timestamp,
    current_timestamp,
    null,
    null,
    'NA',
    0,
    0,
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    0,
    0,
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP,
    'DEFAULT'
)
select
    project_key,
    project_wid,
    project_id,
    project_name,
    worktag_only_ind,
    inactive_ind,
    include_project_id_in_name,
    start_date,
    end_date,
    billable_ind,
    capital_ind,
    description,
    overall_percent_complete,
    probability_of_success,
    external_project_reference_name,
    problem_statement,
    objective,
    in_scope,
    out_of_scope,
    measures_of_success,
    project_overview,
    estimated_budget,
    estimated_revenue,
    realized_revenue,
    project_group,
    project_state,
    project_status,
    company_id,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    project
where 1 = 1     
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where project_wid = project_wid)
{%- endif %}
