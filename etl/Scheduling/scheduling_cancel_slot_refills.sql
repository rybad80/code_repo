select
    cancelled_slots.dept_key,
    cancelled_slots.prov_key,
    cancelled_slots.eff_dt,
    cancelled_slots.specialty_name,
    cancelled_slots.department_name,
    cancelled_slots.provider_name,
    master_date.dt_key,
    cancelled_slots.late_cancel_flag,
    count(backfill_slots_summary.numerator) as refilled_slots,
    coalesce(sum(backfill_slots_summary.refilled_minutes), 0) as refilled_minutes,
    count(cancelled_slots.denominator) as cancelled_slots,
    sum(cancelled_slots.cancelled_minutes) as cancelled_minutes
from
    {{ref('stg_scheduling_cancelled_slots')}} as cancelled_slots
    inner join {{source('cdw', 'master_date')}} as master_date
        on cancelled_slots.eff_dt = master_date.full_dt
    left join {{ref('stg_scheduling_backfill_slots')}} as backfill_slots_summary
        on cancelled_slots.canc_slot_num = backfill_slots_summary.canc_slot_num
group by
    cancelled_slots.dept_key,
    cancelled_slots.prov_key,
    cancelled_slots.eff_dt,
    cancelled_slots.specialty_name,
    cancelled_slots.department_name,
    cancelled_slots.provider_name,
    master_date.dt_key,
    cancelled_slots.late_cancel_flag
