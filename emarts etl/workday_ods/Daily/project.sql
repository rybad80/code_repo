{{ 
    config(
        materialized = 'incremental',
        unique_key = 'project_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['project_wid','project_id','locked_in_workday_ind','project_name','worktag_only_ind','inactive_ind','include_project_id_in_name_ind','start_date','end_date','billable_ind','capital_ind','description','overall_percent_complete','probability','external_project_reference_name','problem_statement','objective','in_scope','out_of_scope','measures_of_success','project_overview','estimated_budget','estimated_revenue','realized_revenue','project_group_id','project_state_id','document_status_id','project_status_id','company_id','md5', 'upd_dt', 'upd_by']
    )
 }}
select distinct
    project_reference_wid as project_wid,
    project_reference_workday_project_id as project_id,
    coalesce(cast(project_data_locked_in_workday as int), -2) as locked_in_workday_ind,
    project_data_project_name as project_name,
    coalesce(cast(project_data_worktag_only as int), -2) as worktag_only_ind,
    coalesce(cast(project_data_inactive as int), -2) as inactive_ind,
    coalesce(cast(project_data_include_project_id_in_name as int), -2) as include_project_id_in_name_ind,
    to_timestamp(project_data_start_date, 'yyyy-mm-dd') as start_date,
    to_timestamp(project_data_end_date, 'yyyy-mm-dd') as end_date,
    coalesce(cast(project_data_billable as int), -2) as billable_ind,
    coalesce(cast(project_data_capital as int), -2) as capital_ind,
    project_data_description as description,
    project_data_overall_percent_complete as overall_percent_complete,
    project_data_probability as probability,
    project_data_external_project_reference_name as external_project_reference_name,
    null as problem_statement,
    null as objective,
    null as in_scope,
    null as out_of_scope,
    null as measures_of_success,
    null as project_overview,
    project_data_estimated_budget as estimated_budget,
    project_data_estimated_revenue as estimated_revenue,
    project_data_realized_revenue as realized_revenue,
    project_group_reference_project_group_id as project_group_id,
    project_state_reference_project_state_id as project_state_id,
    project_event_status_reference_document_status_id as document_status_id,
    project_status_reference_project_status_id as project_status_id,
    company_reference_company_reference_id as company_id,
    cast({{
        dbt_utils.surrogate_key([
            'project_wid',
            'project_id',
            'locked_in_workday_ind',
            'project_name',
            'worktag_only_ind',
            'inactive_ind',
            'include_project_id_in_name_ind',
            'start_date',
            'end_date',
            'billable_ind',
            'capital_ind',
            'description',
            'overall_percent_complete',
            'probability',
            'external_project_reference_name',
            'problem_statement',
            'objective',
            'in_scope',
            'out_of_scope',
            'measures_of_success',
            'project_overview',
            'estimated_budget',
            'estimated_revenue',
            'realized_revenue',
            'project_group_id',
            'project_state_id',
            'document_status_id',
            'project_status_id',
            'company_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
from 
    {{source('workday_ods', 'get_projects')}} as get_projects
where
    get_projects.project_reference_wid is not null
    and 1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                project_wid = get_projects.project_reference_wid
        )
    {%- endif %}   
