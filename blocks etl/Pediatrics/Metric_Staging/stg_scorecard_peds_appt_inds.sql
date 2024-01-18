select
    scheduling_specialty_care_appointments.visit_key as primary_key,
    scheduling_specialty_care_appointments.encounter_date,
    case
        when
            scheduling_specialty_care_appointments.appointment_status_id in ('1', '2', '3', '4', '6')
            and department_care_network.department_id != '62' /*main sleep center*/
            and department_care_network.department_id != '101022016' /*virtua sleep lab*/
            and department_care_network.department_id != '101001076' /*kop sleep center */
            and department_care_network.department_id != '101012070' /*main palliative care*/
            and date_part('hour', scheduling_specialty_care_appointments.appointment_date) >= 16 then 1 else 0
        end as evening_ind,
    case
        when
            appointment_status_id in ('1', '2', '3', '4', '6') and date_part('hour', appointment_date) < 8
            then 1 else 0 end as early_ind, -- 1-scheduled, 2-completed, 3-canceled, 4-no show, 6-arrived
    case
        when
            appointment_status_id in ('1', '2', '3', '4', '6') and date_part('dow', appointment_date) = 1 then 1
        when
            appointment_status_id in ('1', '2', '3', '4', '6') and date_part('dow', appointment_date) = 7
            then 1 else 0 end as weekend_ind,
    case
        when
            scheduling_specialty_care_appointments.appointment_status_id = 4 then 1 else 0
        end as no_show_ind,
    case
		when
            no_show_ind = 1 then past_appointment_ind else 0
        end as no_show_appts,
    scheduling_specialty_care_appointments.past_appointment_ind
from
     {{ref('scheduling_specialty_care_appointments')}} as scheduling_specialty_care_appointments
    inner join {{ref('department_care_network')}} as department_care_network
    on scheduling_specialty_care_appointments.dept_key = department_care_network.dept_key
    inner join {{ source('cdw', 'visit')}} as visit
    on scheduling_specialty_care_appointments.visit_key = visit.visit_key
    left join {{ source('cdw', 'dim_visit_cncl_rsn')}} as dim_visit_cncl_rsn
    on visit.dim_visit_cncl_rsn_key = dim_visit_cncl_rsn.dim_visit_cncl_rsn_key
where
    scheduling_specialty_care_appointments.encounter_date >= '01/01/2019'
    and lower(scheduling_specialty_care_appointments.revenue_location_group) in ('chca')
	and (scheduling_specialty_care_appointments.past_appointment_ind = '1'
        or scheduling_specialty_care_appointments.appointment_status_id = '1')
