{{
    config(
        materialized ='incremental',
        unique_key = 'integration_id',
        meta = {
            'critical': true
        }
    )
}}
select
    workday_plans.company_reference_id,
    coalesce(dim_company.company_key, 0) as company_key,
    workday_plans.plan_reference_id,
    workday_plans.plan_wid,
    workday_plans.plan,
    workday_plans.plan_structure_reference_id,
    workday_plans.organizing_dimension_type,
    workday_plans.plan_type_reference_id,
    workday_plans.inactive,
    workday_plans.is_primary,
    workday_plans.date_from,
    workday_plans.date_to,
    workday_plans.created_moment,
    workday_plans.last_functionally_updated,
    workday_plans.plan_status,
    workday_plans.upd_dt as update_date,
    workday_plans.company_reference_id || '~' || workday_plans.plan_wid as integration_id
from
    {{source('workday_ods', 'workday_plans')}} as workday_plans
	left join {{ref('dim_company')}}  as dim_company
        on dim_company.integration_id = 'WORKDAY~' || workday_plans.company_reference_id
where 1 = 1 
    {%- if is_incremental() %}
    and workday_plans.upd_dt > (select max(update_date) from {{ this }})
    {%- endif %}
