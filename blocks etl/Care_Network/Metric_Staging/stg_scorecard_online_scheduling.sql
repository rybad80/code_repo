select
    stg_encounter.original_appointment_made_date as metric_date,
    stg_encounter.visit_key as primary_key,
    initcap(stg_department_all.specialty_name) as drill_down_one,
    initcap(coalesce(stg_department_all.department_center,
        stg_department_all.department_name)) as drill_down_two,
    stg_encounter.visit_key,
    stg_department_all.specialty_name,
    case when lower(stg_department_all.specialty_name) in ('physical therapy',
        'speech',
        'audiology',
        'occupational therapy',
        'clinical nutrition'
    )
        then 1
        else 0
        end as ancillary_services_ind,
    stg_encounter_outpatient.phys_app_psych_online_scheduled_ind,
    stg_encounter_outpatient.online_scheduled_ind,
    stg_encounter_outpatient.physician_app_psych_visit_ind,
    stg_encounter.walkin_ind
from
    {{ref('stg_encounter')}} as stg_encounter
left join
	{{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
        on stg_encounter_outpatient.visit_key = stg_encounter.visit_key
inner join
	{{ref('stg_department_all')}} as stg_department_all
	on stg_encounter.department_id = stg_department_all.department_id
where
    stg_encounter.walkin_ind = 0
	and stg_encounter.original_appointment_made_date is not null
	and stg_department_all.intended_use_id = 1009
	and lower(stg_department_all.specialty_name) not in (
        'cardiovascular surgery',
        'obstetrics',
        'multidisciplinary',
        'gi/nutrition',
        'family planning'
        )
