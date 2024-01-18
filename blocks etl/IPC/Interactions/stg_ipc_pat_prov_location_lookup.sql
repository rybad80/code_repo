select
    adt_department.visit_key,
    adt_department.enter_date as start_date,
    adt_department.exit_date as end_date,
    adt_department.department_name
from
    {{ref('adt_department')}} as adt_department
union
select
    stg_encounter.visit_key,
    stg_encounter.encounter_date::timestamp as start_date,
    stg_encounter.encounter_date::timestamp + interval'1 day' - interval'1 second' as end_date, --noqa: L048
    stg_encounter.department_name
from
    {{ref('stg_encounter')}} as stg_encounter
    left join {{ref('adt_department')}} as adt_department
        on adt_department.visit_key = stg_encounter.visit_key
where
    stg_encounter.visit_key not in (select visit_key from {{ref('stg_encounter_inpatient')}})
    and adt_department.visit_key is null
