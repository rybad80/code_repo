/* Liver function results. */

{{
    config(materialized = 'view')
}}

{%- set liver_function_components = [
    ([1, 8472, 8980, 10070, 15026, 19287, 20014, 20352, 22285, 999018, 123030006, 123150043], 'albumin'),
    ([2809, 9921, 21403], 'albumin_serum'),
    ([89, 1972, 2814, 10078, 14407, 123030272], 'alt'),
    ([90, 1971, 2813, 10118, 20023, 123030276], 'ast'),
    ([133, 2116, 10374, 12578, 12895, 14406, 20022, 123030025], 'ggt'),
    ([93, 123030280], 'bilirubin_conjugated'),
    ([91, 123030278], 'bilirubin_unconjugated'),
    ([1969, 10153, 20018, 20076], 'bilirubin_direct'),
    ([1970, 10154, 20019, 20077], 'bilirubin_indirect'),
    ([86, 1968, 6055, 9922, 10155, 12577, 12896, 20017, 20075, 999016, 123030219, 123030265], 'total_bilirubin'),
    ([174, 2064, 2868, 7837, 10484, 13481, 17347, 20180, 123040037], 'inr'),
    ([75, 8984, 10651, 16038, 20179, 123040036], 'prothrombin_time')
] %}

with procedure_order_result_value as (
    select
        proc_ord_key,
        {% for comp_id, comp_name in liver_function_components %}
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
            {%- for comp_id, _ in liver_function_components %}
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
    {% for comp_id, comp_name in liver_function_components %}
        {{ comp_name }},
        {{ comp_name }}_numeric{{ "," if not loop.last }}
    {%- endfor %}
from
    procedure_order_result_value
    left join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_result_value.proc_ord_key = procedure_order_clinical.proc_ord_key
