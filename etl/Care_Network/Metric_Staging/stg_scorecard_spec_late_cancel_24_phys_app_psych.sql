select
    'operational' as domain,
    'pfex' as subdomain,
    'Percent 24 hr Cancellation (Physician/APP/Psych Visit)' as metric_name,
    scheduling_specialty_care_appointments.visit_key as primary_key,
    scheduling_specialty_care_appointments.encounter_date as metric_date,
    case when scheduling_specialty_care_appointments.cancel_24hr_ind = 1 then 1 else 0 end as num,
    scheduling_specialty_care_appointments.past_appointment_ind as denom,
    'sum' as num_calculation,
    'sum' as denom_calculation,
    'percentage' as metric_type,
    'down' as desired_direction,
    'spec_late_cancel_24_phys_app_psych' as metric_id,
    initcap(scheduling_specialty_care_appointments.specialty_name) as drill_down_one,
    initcap(coalesce(department_care_network.department_center,
    scheduling_specialty_care_appointments.department_name)) as drill_down_two
from
    {{ ref('scheduling_specialty_care_appointments') }} as scheduling_specialty_care_appointments
    inner join {{ ref('department_care_network') }} as department_care_network
    on scheduling_specialty_care_appointments.dept_key = department_care_network.dept_key
where
    scheduling_specialty_care_appointments.physician_app_psych_visit_ind = 1
    and lower(scheduling_specialty_care_appointments.specialty_name) not in
    ('physical therapy', 'speech', 'audiology', 'occupational therapy', 'clinical nutrition')
    and scheduling_specialty_care_appointments.encounter_date
    >= date_trunc('year', current_date - interval '5 year')
