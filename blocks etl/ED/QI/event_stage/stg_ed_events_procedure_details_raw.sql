select
    cohort.visit_key,
    lookup.event_category,
    lookup.event_name,
    lookup.event_selection_type,
    procedure_order_result_clinical.placed_date,
    procedure_order_result_clinical.specimen_taken_date,
    procedure_order_result_clinical.result_date,
    min(procedure_order_result_clinical.result_date) over(
        partition by procedure_order_result_clinical.procedure_order_id
    ) as proc_order_first_result_ts,
    procedure_order_result_clinical.procedure_order_id,
    procedure_order_result_clinical.result_seq_num,
    procedure_order_result_clinical.result_component_id,
    procedure_order_result_clinical.order_specimen_source as specimen_source,
    procedure_order_result_clinical.result_status,
    procedure_order_result_clinical.result_lab_status,
    procedure_order_result_clinical.result_value
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    inner join {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
        on cohort.visit_key = procedure_order_result_clinical.visit_key
    inner join {{ ref('lookup_ed_events_procedure_details') }} as lookup
        on (
            procedure_order_result_clinical.procedure_id = lookup.procedure_id
            or lookup.procedure_id is null
        )
        and (
            upper(procedure_order_result_clinical.procedure_name) like upper(lookup.procedure_pattern)
            or lookup.procedure_pattern is null
        )
        and (
            procedure_order_result_clinical.result_component_id = lookup.result_component_id
            or lookup.result_component_id is null
        )
        and (
            upper(procedure_order_result_clinical.result_component_name) like upper(
                lookup.result_component_pattern
            )
            or lookup.result_component_pattern is null
        )
        and (
            upper(procedure_order_result_clinical.result_value) like upper(lookup.result_pattern)
            or lookup.result_pattern is null
        )
        and (
            upper(procedure_order_result_clinical.order_specimen_source) like upper(lookup.specimen_pattern)
            or lookup.specimen_pattern is null
        )
where
    (
        '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ed/%'
        and procedure_order_result_clinical.placed_date <= coalesce(
            cohort.disch_ed_dt,
            cohort.depart_ed_dt,
            current_date
        )
    )
    or (
        '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/edecu/%'
        and procedure_order_result_clinical.placed_date between
        cohort.admit_edecu_dt and cohort.disch_edecu_dt
    )
    or (
        '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ip/%'
        and procedure_order_result_clinical.placed_date  >= coalesce(
            cohort.disch_edecu_dt,
            cohort.disch_ed_dt,
            cohort.depart_ed_dt
        )
    )
