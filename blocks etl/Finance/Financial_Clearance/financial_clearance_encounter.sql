select
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.appointment_date,
    stg_encounter.mrn,
    stg_encounter.appointment_status_id,
    stg_encounter.department_id,
    stg_department_all.department_center_abbr,
    case when coalesce(stg_encounter_outpatient_raw.primary_care_ind, 0) = 0
        and (
                lookup_finance_visit_type.visit_type_id is not null
                or lookup_finance_encounter_type.encounter_type_id is not null
            )
        and (
            lower(clarity_dep.external_name) not like '%primary care%'
            and lower(external_name) not like '%care network%'
            and lower(clarity_dep.department_name) not like '%care ntwk%'
            and lower(clarity_dep.department_name) not like '%family plan%'
            and lower(clarity_dep.department_name) not like '%fam plan%'
        )
        then 1
    else
        0
    end as financial_clearance_ind
from
    {{ref('stg_encounter')}} as stg_encounter
inner join {{ref('stg_department_all')}} as stg_department_all
    on stg_department_all.dept_key = stg_encounter.dept_key
left join {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
    on stg_encounter_outpatient_raw.visit_key = stg_encounter.visit_key
left join {{ ref('lookup_finance_encounter_type') }} as lookup_finance_encounter_type
    on stg_encounter.encounter_type_id = lookup_finance_encounter_type.encounter_type_id
left join {{ ref('lookup_finance_visit_type') }} as lookup_finance_visit_type
    on stg_encounter.visit_type_id = cast(lookup_finance_visit_type.visit_type_id as varchar(6))
left join {{ source('clarity_ods', 'clarity_dep') }} as clarity_dep
    on stg_encounter.department_id = clarity_dep.department_id
