select
    {{
    dbt_utils.surrogate_key([
        'scheduling_cancel_slot_refills.dept_key',
        'scheduling_cancel_slot_refills.prov_key',
        'scheduling_cancel_slot_refills.eff_dt',
        ])
    }} as primary_key,
    scheduling_cancel_slot_refills.dept_key,
    scheduling_cancel_slot_refills.prov_key,
    scheduling_cancel_slot_refills.eff_dt,
    scheduling_cancel_slot_refills.specialty_name as specialty,
    scheduling_cancel_slot_refills.department_name as dept_nm,
    scheduling_cancel_slot_refills.provider_name as prov_nm,
    scheduling_cancel_slot_refills.dt_key,
    scheduling_cancel_slot_refills.late_cancel_flag,
    scheduling_cancel_slot_refills.refilled_slots,
    scheduling_cancel_slot_refills.refilled_minutes,
    scheduling_cancel_slot_refills.cancelled_slots,
    scheduling_cancel_slot_refills.cancelled_minutes
from
    {{ ref('scheduling_cancel_slot_refills')}} as scheduling_cancel_slot_refills
    left join {{ ref('department_care_network')}} as department_care_network
    on scheduling_cancel_slot_refills.dept_key = department_care_network.dept_key
where
    late_cancel_flag in ('24hr', '48hr')
    and eff_dt >= '01/01/2019'
    and lower(department_care_network.revenue_location_group) = 'chca'
