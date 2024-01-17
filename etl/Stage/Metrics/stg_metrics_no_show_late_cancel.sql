{{ config(meta = {
    'critical': false
}) }}

with office_visit_ind as (
    select
        stg_office_visit_grouper.department_id,
        max(stg_office_visit_grouper.physician_app_psych_visit_ind) as physician_app_psych_visit_ind
    from
        {{ref('stg_office_visit_grouper')}} as stg_office_visit_grouper
    group by
        stg_office_visit_grouper.department_id
)
select
    'operational' as domain, -- noqa: L029
    'pfex' as subdomain,
    'Percent 48 hr Cancellation/No Shows' as metric_name,
    scheduling_specialty_care_appointments.visit_key as primary_key,
    scheduling_specialty_care_appointments.encounter_date as metric_date,
    case
        when scheduling_specialty_care_appointments.appointment_status_id = 4
            or scheduling_specialty_care_appointments.cancel_48hr_ind = 1
        then 1
        else 0
        end as num,
    scheduling_specialty_care_appointments.past_appointment_ind as denom,
    'sum' as num_calculation,
    'sum' as denom_calculation,
    'percentage' as metric_type,
    'down' as desired_direction,
    'enterprise_late_cancel_no_show' as metric_id,
    initcap(scheduling_specialty_care_appointments.specialty_name) as specialty_name,
    scheduling_specialty_care_appointments.department_name,
    scheduling_specialty_care_appointments.dept_key,
    case
        when lower(department_care_network.revenue_location_group) in ('chca', 'csa')
        then 1
        else 0
        end as chca_csa_ind,
    case
            when lower(scheduling_specialty_care_appointments.specialty_name) in (
                'physical therapy',
                'speech',
                'audiology',
                'occupational therapy',
                'clinical nutrition'
            )
            then 1
            else 0
        end as ancillary_services_ind,
    scheduling_specialty_care_appointments.physician_app_psych_visit_ind,
    initcap(coalesce(department_care_network.department_center,
        scheduling_specialty_care_appointments.department_name)) as department_center
from
    {{ref('scheduling_specialty_care_appointments')}} as scheduling_specialty_care_appointments
inner join
    {{ref('department_care_network')}} as department_care_network
    on scheduling_specialty_care_appointments.dept_key = department_care_network.dept_key
left join
    office_visit_ind
    on department_care_network.department_id = office_visit_ind.department_id
