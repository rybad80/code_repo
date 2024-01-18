select
    cohort.visit_key,
    lookup.event_category,
    lookup.event_name || '_history' as event_name,
    'medication_history' as event_source,
    coalesce(
        medication_history.administration_date,
        medication_history.medication_start_date
    ) as event_timestamp,
    medication_history.medication_name as meas_val,
    row_number() over (
        partition by
            cohort.visit_key,
            lookup.event_name
        order by event_timestamp desc
    ) as event_repeat_number

from
    {{ ref('stg_ed_encounter_cohort_all') }} as cohort
    inner join {{ ref('medication_order_administration') }} as medication_history
        on medication_history.pat_key = cohort.pat_key
    inner join {{ ref('lookup_ed_events_medication_history') }} as lookup
        on (
            upper(medication_history.medication_name) like lookup.pattern
            or upper(medication_history.generic_medication_name) like lookup.pattern
        )
where
    medication_history.visit_key != cohort.visit_key
    --ignore incorrect medications
    and medication_history.medication_end_date > medication_history.medication_start_date
    and (
        (
            medication_history.discharge_med_ind = 1 --discharge medication
            and medication_history.medication_start_date < cohort.arrive_ed_dt
            and medication_history.medication_start_date + cast(
                lookup.time_interval as interval
            ) > cohort.arrive_ed_dt
        )
        or (
            medication_history.administration_date is not null --administered in hospital
            and medication_history.administration_date < cohort.arrive_ed_dt
            and medication_history.administration_date + cast(
                lookup.time_interval as interval
            ) > cohort.arrive_ed_dt
        )
    )
