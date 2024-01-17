/* Coda Lab results. */

{{
    config(materialized = 'view')
}}

{%- set coda_lab_components = [
    ([76, 2063, 2177,2178, 2869, 3285, 3843, 8213, 8723, 10112, 20185, 21159, 123040038, 123040039], 'partial_thromboplastin_time'),
    ([5119, 501719, 123130062, 123130095], 'activated_coagulation_time')
] %}

with procedure_order_result_value as (
    select
        proc_ord_key,
        {% for comp_id, comp_name in coda_lab_components %}
            max(
                case
                    when
                        {%- for id in comp_id %}
                            result_component_id = {{ id }} {{ "or" if not loop.last -}}
                        {%- endfor %}
                        then result_value
                    else null
                end
            ) as {{ comp_name }},
            max(
                case
                    when
                        {%- for id in comp_id %}
                            result_component_id = {{ id }} {{ "or" if not loop.last -}}
                        {%- endfor %}
                        then result_value_numeric
                    else null
                end
            ) as {{ comp_name }}_numeric{{ "," if not loop.last }}   
        {%- endfor %}
    from
        {{ ref('procedure_order_result_clinical') }}
    where
        result_component_id in (
            {%- for comp_id, _ in coda_lab_components %}
                {%- for id in comp_id %}
                        {{ id }}{{ "," if not loop.last }}
                {%- endfor %}{{ "," if not loop.last }}
            {%- endfor %}
        )
        and result_value != 'ND'
    group by
        proc_ord_key
)

select
    procedure_order_clinical.proc_ord_key,
    procedure_order_clinical.pat_id,
    procedure_order_clinical.pat_key,
    procedure_order_clinical.patient_name,
    procedure_order_clinical.mrn,
    procedure_order_clinical.dob,
    procedure_order_clinical.visit_key,
    procedure_order_clinical.order_specimen_source,
    procedure_order_clinical.specimen_taken_date,
    procedure_order_clinical.procedure_name,
    procedure_order_clinical.procedure_group_name,
    procedure_order_clinical.procedure_subgroup_name,
    procedure_order_clinical.result_date,
    procedure_order_clinical.parent_placed_date,
    {% for comp_id, comp_name in coda_lab_components %}
        {{ comp_name }},
        {{ comp_name }}_numeric{{ "," if not loop.last }}
    {%- endfor %}
from
    procedure_order_result_value
    left join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_result_value.proc_ord_key = procedure_order_clinical.proc_ord_key
