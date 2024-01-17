{{ config(meta = {
    'critical': false
}) }}

with all_backfill_slots as (
    select
        canc_slot_num,
        stg_encounter.appointment_date
            + cast(stg_encounter.scheduled_length_min || ' minute' as interval) as appt_end_dt,
        case
            when stg_encounter.appointment_date < canc_appt_dt --non-cancelled appt begins before cancelled slot
                then canc_appt_dt else stg_encounter.appointment_date
            end as bckfl_appt_dt,
        case
            when appt_end_dt > canc_slot_end_dt --non-cancelled appt ends after cancelled slot
                then canc_slot_end_dt else appt_end_dt
            end as bckfl_end_dt,
        extract(hour from (bckfl_end_dt - bckfl_appt_dt)) * 60
            + extract(minute from (bckfl_end_dt - bckfl_appt_dt)) as bckfl_lgth_min,
        lead(bckfl_appt_dt, 1) over (partition by canc_slot_num
                                        order by bckfl_appt_dt, bckfl_lgth_min) as next_bckfl_dt,
        case
            when next_bckfl_dt is null --latest appointment for prov/dept/day
                then bckfl_lgth_min
            when bckfl_end_dt <= next_bckfl_dt --no overlap with next appointment
                then bckfl_lgth_min
            when bckfl_appt_dt = next_bckfl_dt --same start time as next appointment
                then 0
            else extract(hour from (next_bckfl_dt - bckfl_appt_dt)) * 60
                + extract(minute from (next_bckfl_dt - bckfl_appt_dt))
        --else condition is appt_end_dt > next_dt, which guarantees overlap with next appointment (opposite of #2)
            end as refilled_minutes
        from
            {{ref('stg_encounter')}} as stg_encounter
            inner join {{ref('stg_scheduling_cancelled_slots')}} as cancelled_slots
                on stg_encounter.dept_key = cancelled_slots.dept_key
                and stg_encounter.prov_key = cancelled_slots.prov_key
                and stg_encounter.eff_dt = cancelled_slots.eff_dt
        where
            stg_encounter.appointment_status_id != '3' --not cancelled
            and stg_encounter.encounter_type_id in ('50', '101') --appt or office stg_encounter
            and stg_encounter.patient_class_id in ('0', '2', '5', '6') --N/A, OP, Obsv, Recurr OP
            and ((stg_encounter.appointment_date >= canc_appt_dt
                        and stg_encounter.appointment_date < canc_slot_end_dt)
                --non-cancelled appt begins in cancelled slot
                or --noqa
                ((stg_encounter.appointment_date
                    + cast(stg_encounter.scheduled_length_min || ' minute' as interval)) > canc_appt_dt
                and (stg_encounter.appointment_date
                    + cast(stg_encounter.scheduled_length_min || ' minute' as interval)) <= canc_slot_end_dt)
                --non-cancelled appt ends in cancelled slot
                )
)

select
    canc_slot_num,
    1 as numerator,
    sum(refilled_minutes) as refilled_minutes
from
    all_backfill_slots
group by
    canc_slot_num
