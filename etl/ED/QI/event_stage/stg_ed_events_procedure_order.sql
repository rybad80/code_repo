select
    cohort.visit_key,
    lookup.event_category,
    lookup.event_name,
    'procedure_order_clinical' as event_source,
    case
        when lower(lookup.event_timestamp_selection) like '%result%'
            then procedure_order_clinical.result_date
        when lower(lookup.event_timestamp_selection) like '%specimen%'
            then procedure_order_clinical.specimen_taken_date
        else procedure_order_clinical.placed_date
    end as event_timestamp,
    procedure_order_clinical.procedure_name as meas_val,
    case
        when lower(lookup.event_selection_type) = 'last'
            then row_number() over (
              partition by
                  cohort.visit_key,
                  lookup.event_name
              order by
                  event_timestamp desc nulls last,
                  procedure_order_clinical.procedure_order_id desc
            )
        else row_number() over (
              partition by
                  cohort.visit_key,
                  lookup.event_name
              order by
                  event_timestamp asc nulls last,
                  procedure_order_clinical.procedure_order_id asc
            )
    end as event_repeat_number,
    case
        when procedure_order_clinical.proc_ord_parent_key is not null
            and procedure_order_clinical.proc_ord_parent_key != 0
        then procedure_order_clinical.proc_ord_parent_key
        else procedure_order_clinical.proc_ord_key
    end as proc_ord_root_key
from
    {{ ref('stg_ed_encounter_cohort_all') }} as cohort
    inner join
        {{ ref('procedure_order_clinical') }} as procedure_order_clinical
            on procedure_order_clinical.visit_key = cohort.visit_key
    inner join {{ ref('lookup_ed_events_procedure_order_clinical') }} as lookup
        on (
            lookup.procedure_id = procedure_order_clinical.procedure_id
            or lookup.procedure_id is null
        )
        and (
            upper(procedure_order_clinical.procedure_name) like upper(lookup.pattern)
            or lookup.pattern is null
        )
        and (
            upper(procedure_order_clinical.order_specimen_source) like upper(lookup.specimen_pattern)
            or lookup.specimen_pattern is null
        )
where
    (
        (
            '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ed/%'
            and procedure_order_clinical.placed_date
                <= coalesce(cohort.disch_ed_dt, cohort.depart_ed_dt, current_date)
        )
        or (
            '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/edecu/%'
            and procedure_order_clinical.placed_date between cohort.admit_edecu_dt and cohort.disch_edecu_dt
        )
        or (
            '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ip/%'
            and procedure_order_clinical.placed_date  >= coalesce(
                cohort.disch_edecu_dt,
                cohort.disch_ed_dt,
                cohort.depart_ed_dt
            )
        )
    )
    and (
        lookup.order_status_exclude is null
        or lookup.order_status_exclude = ''
        or lower(lookup.order_status_exclude) not like ('%' || lower(procedure_order_clinical.order_status) || '%')
    )
