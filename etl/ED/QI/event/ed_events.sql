
{% set refs = [
    ref('stg_ed_events_pathway_order'),
    ref('stg_ed_events_medication_history'),
    ref('stg_ed_events_medication_order'),
    ref('stg_ed_events_medication_details'),
    ref('stg_ed_events_procedure_order')
] %}

{{ dbt_utils.union_relations(
    relations = refs,
    exclude = ['PROC_ORD_ROOT_KEY'],
    column_override = {
    "EVENT_NAME": "varchar(150)",
    "EVENT_CATEGORY":"varchar(150)",
    "EVENT_SOURCE":"varchar(150)",
    "MEAS_VAL":"varchar(200)"},
    source_column_name="dbt_source_relation") }}
