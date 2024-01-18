select
    encounter_primary_care.visit_key,
    encounter_primary_care.visit_key as action_key,
    1 as action_seq_num,
    encounter_primary_care.prov_key,
    coalesce(encounter_primary_care.provider_name, provider.full_nm) as provider_name,
    employee.emp_key,
    provider_name as employee_name,
    encounter_primary_care.encounter_date as event_date,
    'primary care encounter' as event_description,
    department_name as event_location,
    'visit_key' as action_key_field
from
    {{ref('encounter_primary_care')}} as encounter_primary_care
        left join {{source('cdw', 'employee')}} as employee
            on employee.prov_key = encounter_primary_care.prov_key
            and encounter_primary_care.prov_key != 0
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = encounter_primary_care.prov_key
where
    encounter_primary_care.prov_key != 0
union all
select
     encounter_specialty_care.visit_key,
     encounter_specialty_care.visit_key as action_key,
    1 as action_seq_num,
    encounter_specialty_care.prov_key,
    coalesce(encounter_specialty_care.provider_name, provider.full_nm) as provider_name,
    employee.emp_key,
    coalesce(encounter_specialty_care.provider_name, employee.full_nm) as employee_name,
    encounter_specialty_care.encounter_date as event_date,
    'specialty care encounter' as event_description,
    department_name as event_location,
    'visit_key' as action_key_field
from
    {{ref('encounter_specialty_care')}} as encounter_specialty_care
        left join {{source('cdw', 'employee')}} as employee
            on employee.prov_key = encounter_specialty_care.prov_key
            and encounter_specialty_care.prov_key != 0
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = encounter_specialty_care.prov_key
where
    encounter_specialty_care.prov_key != 0
