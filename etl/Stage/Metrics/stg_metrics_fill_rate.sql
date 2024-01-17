{{ config(meta = {
    'critical': false
}) }}

select
    'clinical' as domain,  -- noqa: L029
    'Fill Rate' as metric_name,
    {{
        dbt_utils.surrogate_key([
        'scheduling_provider_slot_status.prov_key',
        'scheduling_provider_slot_status.slot_start_time',
        'scheduling_provider_slot_status.dept_key'
        ])
    }} as primary_key,
    scheduling_provider_slot_status.encounter_date as metric_date,
    scheduling_provider_slot_status.scheduled_ind as num,
    scheduling_provider_slot_status.available_ind as denom,
    'sum' as num_calculation,
    'sum' as denom_calculation,
    'percentage' as metric_type,
    'up' as desired_direction,
    'enterprise_fill_rate' as metric_id,
    initcap(scheduling_provider_slot_status.specialty_name) as specialty_name,
    scheduling_provider_slot_status.department_name,
    scheduling_provider_slot_status.dept_key,
    initcap(coalesce(department_care_network.department_center,
        scheduling_provider_slot_status.department_name)) as department_center,
    department_care_network.revenue_location_group,
    case
        when lower(department_care_network.revenue_location_group) in ('chca', 'csa')
        then 1
        else 0
        end as chca_csa_ind,
    scheduling_provider_slot_status.specialty_care_slot_ind,
    scheduling_provider_slot_status.primary_care_slot_ind,
    scheduling_provider_slot_status.ancillary_services_ind,
    scheduling_provider_slot_status.evening_appointment_ind
from
    {{ref('scheduling_provider_slot_status')}} as scheduling_provider_slot_status
inner join
    {{ref('department_care_network')}} as department_care_network
    on scheduling_provider_slot_status.dept_key = department_care_network.dept_key
where
    scheduling_provider_slot_status.fill_rate_incl_ind = 1
    and scheduling_provider_slot_status.encounter_date < current_date
