{{
    config(
        materialized = 'incremental',
        unique_key = 'worker_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['worker_wid','worker_id', 'user_id', 'universal_id', 'employee_id', 'contingent_worker_id', 'company_id', 'cost_center_id', 'cost_center_site_id', 'location_hierarchy_id', 'department_id', 'provider_id', 'pay_group_id', 'kronos_timekeeper_id','kronos_scheduler_id', 'glcode_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with base_workers_organizations as (
    select distinct
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.worker_data_worker_id as worker_id,
        get_workers_organization_data.worker_data_user_id as user_id,
        get_workers_organization_data.worker_data_universal_id as universal_id,
        cast(cast(get_workers_organization_data.worker_reference_employee_id as int) as varchar(50)) as employee_id,
        cast(cast(get_workers_organization_data.worker_reference_contingent_worker_id as int) as varchar(50)) as contingent_worker_id
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
),
worker_organization_data_company as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as company_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Company'
),
worker_organization_data_costcenter as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as cost_center_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Cost_Center'
),
worker_organization_data_costcentersite as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as cost_center_site_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Cost_Center_Site'
),
worker_organization_data_location as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as location_hierarchy_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Location_Hierarchy'
        and get_workers_organization_data.organization_subtype_reference_organization_subtype_id = 'Location'
        and get_workers_organization_data.organization_data_organization_code is not null
),
worker_organization_data_supervisory_dept as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as department_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Supervisory'
        and get_workers_organization_data.organization_subtype_reference_organization_subtype_id = 'Department'
),
worker_organization_data_provider as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as provider_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'ORGANIZATION_TYPE-6-57'
        and get_workers_organization_data.organization_subtype_reference_organization_subtype_id = 'Provider'
),
worker_organization_data_paygroup as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as pay_group_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Pay_Group'
        and get_workers_organization_data.organization_subtype_reference_organization_subtype_id = 'Pay_Group'
),
worker_organization_data_kronos_timekeeper as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as kronos_timekeeper_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Kronos_Timekeeper'
        and get_workers_organization_data.organization_subtype_reference_organization_subtype_id = 'Payroll_Kronos'
),
worker_organization_data_kronos_sched as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as kronos_scheduler_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'Kronos_Scheduler'
        and get_workers_organization_data.organization_subtype_reference_organization_subtype_id = 'Payroll_Kronos'
),
worker_organization_data_kronos_glcode as (
    select
        get_workers_organization_data.worker_reference_wid as worker_wid,
        get_workers_organization_data.organization_data_organization_reference_id as glcode_id,
        get_workers_organization_data.organization_data_organization_code as organization_code,
        get_workers_organization_data.organization_data_organization_name as organization_name
    from
        {{source('workday_ods', 'get_workers_organization_data')}} as get_workers_organization_data
    where
        get_workers_organization_data.organization_type_reference_organization_type_id = 'ORGANIZATION_TYPE-6-37'
        and get_workers_organization_data.organization_subtype_reference_organization_subtype_id = 'ORGANIZATION_SUBTYPE-6-21'
),
worker_org as (
    select distinct
        base_workers_organizations.worker_wid,
        base_workers_organizations.worker_id,
        base_workers_organizations.user_id,
        base_workers_organizations.universal_id,
        base_workers_organizations.employee_id,
        base_workers_organizations.contingent_worker_id,
        worker_organization_data_company.company_id,
        worker_organization_data_costcenter.cost_center_id,
        worker_organization_data_costcentersite.cost_center_site_id,
        worker_organization_data_location.location_hierarchy_id,
        worker_organization_data_supervisory_dept.department_id,
        worker_organization_data_provider.provider_id,
        worker_organization_data_paygroup.pay_group_id,
        worker_organization_data_kronos_timekeeper.kronos_timekeeper_id,
        worker_organization_data_kronos_sched.kronos_scheduler_id,
        worker_organization_data_kronos_glcode.glcode_id,
        cast({{
            dbt_utils.surrogate_key([
                'base_workers_organizations.worker_wid',
                'worker_id',
                'user_id',
                'universal_id',
                'employee_id',
                'contingent_worker_id',
                'company_id',
                'cost_center_id',
                'cost_center_site_id',
                'location_hierarchy_id',
                'department_id',
                'provider_id',
                'pay_group_id',
                'kronos_timekeeper_id',
                'kronos_scheduler_id',
                'glcode_id',
    
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        base_workers_organizations
    left join worker_organization_data_company on
        base_workers_organizations.worker_wid = worker_organization_data_company.worker_wid
    left join worker_organization_data_costcenter on
        base_workers_organizations.worker_wid = worker_organization_data_costcenter.worker_wid
    left join worker_organization_data_costcentersite on
        base_workers_organizations.worker_wid = worker_organization_data_costcentersite.worker_wid
    left join worker_organization_data_location on
        base_workers_organizations.worker_wid = worker_organization_data_location.worker_wid
    left join worker_organization_data_supervisory_dept on
        base_workers_organizations.worker_wid = worker_organization_data_supervisory_dept.worker_wid
    left join worker_organization_data_provider on
        base_workers_organizations.worker_wid = worker_organization_data_provider.worker_wid
    left join worker_organization_data_paygroup on
        base_workers_organizations.worker_wid = worker_organization_data_paygroup.worker_wid
    left join worker_organization_data_kronos_timekeeper on
        base_workers_organizations.worker_wid = worker_organization_data_kronos_timekeeper.worker_wid
    left join worker_organization_data_kronos_sched on
        base_workers_organizations.worker_wid = worker_organization_data_kronos_sched.worker_wid
    left join worker_organization_data_kronos_glcode on
        base_workers_organizations.worker_wid = worker_organization_data_kronos_glcode.worker_wid
)
select
    worker_wid,
    worker_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    company_id,
    cost_center_id,
    cost_center_site_id,
    location_hierarchy_id,
    department_id,
    provider_id,
    pay_group_id,
    kronos_timekeeper_id,
    kronos_scheduler_id,
    glcode_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_org
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                worker_wid = worker_org.worker_wid
        )
    {%- endif %}
