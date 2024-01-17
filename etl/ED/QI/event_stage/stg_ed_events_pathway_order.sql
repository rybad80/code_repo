select
    cohort.visit_key,
    lookup.event_category,
    lookup.event_name,
    'pathway_order' as event_source,
    procedure_order_clinical.placed_date as event_timestamp,
    null as meas_val,
    row_number() over (
        partition by
            cohort.visit_key,
            lookup.event_name
        order by event_timestamp
    ) as event_repeat_number

from
    {{ ref('stg_ed_encounter_cohort_all') }} as cohort
    inner join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_clinical.visit_key = cohort.visit_key
    inner join {{ ref('lookup_ed_events_pathway_order') }} as lookup
        on lookup.procedure_id = procedure_order_clinical.procedure_id
where
    lower(procedure_order_clinical.procedure_order_type) = 'child order'
