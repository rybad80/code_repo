select
    cohort.visit_key,
    lookup.event_category,
    lookup.event_name || '_ordered' as event_name,
    'medication_order_administration' as event_source,
    medication_order_administration.medication_order_create_date as event_timestamp,
    medication_order_administration.medication_name as meas_val,
    row_number() over (
        partition by
            cohort.visit_key,
            lookup.event_name
        order by event_timestamp
    ) as event_repeat_number

from
    {{ ref('stg_ed_encounter_cohort_all') }} as cohort
    inner join {{ ref('medication_order_administration') }} as medication_order_administration
        on medication_order_administration.visit_key = cohort.visit_key
    inner join {{ ref('lookup_ed_events_medication_order_administration') }} as lookup
        on (upper(medication_order_administration.medication_name) like lookup.pattern
        or upper(medication_order_administration.generic_medication_name) like lookup.pattern)
where
    medication_order_administration.medication_order_create_date <= cohort.depart_ed_dt
    and discharge_med_ind = 0

union all

select
    cohort.visit_key,
    lookup.event_category,
    lookup.event_name || '_administered' as event_name,
    'medication_order_administration' as event_source,
    medication_order_administration.administration_date as event_timestamp,
    medication_order_administration.medication_name as meas_val,
    row_number() over (
        partition by
            cohort.visit_key,
            lookup.event_name
        order by event_timestamp
    ) as event_repeat_number

from
    {{ ref('stg_ed_encounter_cohort_all') }} as cohort
    inner join {{ ref('medication_order_administration') }} as medication_order_administration
                on medication_order_administration.visit_key = cohort.visit_key
    inner join {{ ref('lookup_ed_events_medication_order_administration') }} as lookup
        on (upper(medication_order_administration.medication_name) like lookup.pattern
        or upper(medication_order_administration.generic_medication_name) like lookup.pattern)
where
    medication_order_administration.administration_date <= cohort.depart_ed_dt
    and medication_order_administration.administration_date is not null

union all

select
    cohort.visit_key,
    lookup.event_category,
    lookup.event_name || '_discharge_med' as event_name,
    'medication_order_administration' as event_source,
    medication_order_administration.medication_order_create_date as event_timestamp,
    medication_order_administration.medication_name as meas_val,
    row_number() over (
        partition by
            cohort.visit_key,
            lookup.event_name
        order by event_timestamp
    ) as event_repeat_number

from
    {{ ref('stg_ed_encounter_cohort_all') }} as cohort
    inner join {{ ref('medication_order_administration') }} as medication_order_administration
        on medication_order_administration.visit_key = cohort.visit_key
    inner join {{ ref('lookup_ed_events_medication_order_administration') }} as lookup
        on (upper(medication_order_administration.medication_name) like lookup.pattern
        or upper(medication_order_administration.generic_medication_name) like lookup.pattern)
where
    discharge_med_ind = 1
