with latest_provs as (
    select
        employee.prov_key,
        max(employee.emp_key) as emp_key
    from
        {{source('cdw', 'employee')}} as employee
    group by
        employee.prov_key
)

select
    provider.prov_key,
    employee.emp_key,
    coalesce(employee.full_nm, provider.full_nm) as employee_name
from
    {{source('cdw', 'provider')}} as provider
    left join latest_provs on latest_provs.prov_key = provider.prov_key
    left join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = latest_provs.emp_key
where
    provider.prov_key != 0
