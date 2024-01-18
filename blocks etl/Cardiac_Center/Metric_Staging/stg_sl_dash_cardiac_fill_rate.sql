select  
    {{
    dbt_utils.surrogate_key([
        'slot.provider_id',
        'slot.slot_start_time',
        'stg_department_all.department_id'
        ])
    }} as primary_key,
    slot.specialty_name,
    slot.department_name,
    coalesce(stg_department_all.department_center, slot.department_name) as site,
    slot.encounter_date,
    slot.slot_start_time,
    date(slot_start_time) as metric_date,
    slot.scheduled_ind as num,
    slot.available_ind as denom,
    'cardiac_fill_rate' as metric_id
from
    {{ ref('scheduling_provider_slot_status') }} as slot
    inner join {{ ref('stg_department_all') }} as stg_department_all 
        on stg_department_all.dept_key = slot.dept_key
where	
    specialty_care_slot_ind = 1
    and fill_rate_incl_ind = 1
    and lower(slot.specialty_name) = 'cardiology'
