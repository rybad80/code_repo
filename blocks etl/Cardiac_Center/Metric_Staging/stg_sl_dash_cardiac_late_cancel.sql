select
    scheduling_specialty_care_appointments.specialty_name,
    scheduling_specialty_care_appointments.department_name,
    scheduling_specialty_care_appointments.visit_key,
    coalesce(department_care_network.department_center, department_care_network.department_name) as site,
    scheduling_specialty_care_appointments.encounter_date,
    case when
        lower(scheduling_specialty_care_appointments.appointment_status_id) = 4 --no show
        or scheduling_specialty_care_appointments.cancel_48hr_ind = 1 then 1 else 0 end as num,
    case when scheduling_specialty_care_appointments.past_appointment_ind = 1 then 1 else 0 end as denom,
    scheduling_specialty_care_appointments.visit_key as primary_key,
    'cardiac_48_canc_no_show' as metric_id
from
    {{ ref('scheduling_specialty_care_appointments') }} as scheduling_specialty_care_appointments
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = scheduling_specialty_care_appointments.visit_key
        and lower(stg_encounter.patient_class) != 'observation'
    inner join {{ ref('department_care_network') }} as department_care_network
        on scheduling_specialty_care_appointments.department_id = department_care_network.department_id
where
    lower(scheduling_specialty_care_appointments.specialty_name) = 'cardiology'
    and scheduling_specialty_care_appointments.encounter_date < current_date
