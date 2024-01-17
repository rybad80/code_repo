select
    encounter_specialty_care.encounter_date as metric_date,
    encounter_specialty_care.visit_key as primary_key,
    initcap(encounter_specialty_care.specialty_name) as drill_down_one,
    initcap(coalesce(department_care_network.department_center,
        encounter_specialty_care.department_name)) as drill_down_two,
    encounter_specialty_care.visit_key,
    case when lower(encounter_specialty_care.specialty_name) in ('physical therapy',
        'speech',
        'audiology',
        'occupational therapy',
        'clinical nutrition'
    )
        then 1
        else 0
        end as ancillary_services_ind,
    case when lower(department_care_network.revenue_location_group) in ('chca', 'csa')
        then 1
        else 0
        end as chca_csa_ind,
    encounter_specialty_care.mychop_scheduled_ind,
    encounter_specialty_care.physician_app_psych_visit_ind
from
    {{ref('encounter_specialty_care')}} as encounter_specialty_care
inner join {{ref('department_care_network')}} as department_care_network
    on encounter_specialty_care.dept_key = department_care_network.dept_key
inner join {{ref('scheduling_specialty_care_appointments')}} as scheduling_specialty_care_appointments
	on encounter_specialty_care.visit_key = scheduling_specialty_care_appointments.visit_key
where
    encounter_specialty_care.encounter_date < current_date
