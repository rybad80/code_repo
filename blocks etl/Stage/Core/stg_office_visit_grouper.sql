{{ config(meta = {
    'critical': true
}) }}

select distinct
    cast(lookup_office_visit_grouper.visit_type_id as varchar(30)) as visit_type_id,
    lookup_office_visit_grouper.visit_type,
    lookup_office_visit_grouper.department_name,
    stg_department_all.department_id,
    lookup_office_visit_grouper.specialty_name,
    case
        when lower(lookup_office_visit_grouper.physician_ind) like '%yes%'
            then 1
            else 0
        end as physician_app_psych_visit_ind
from
    {{ ref('lookup_office_visit_npv_consolidated_grouper') }} as lookup_office_visit_grouper
left join
    {{ ref('stg_department_all') }} as stg_department_all
    on lookup_office_visit_grouper.department_id = stg_department_all.department_id
where physician_ind is not null
