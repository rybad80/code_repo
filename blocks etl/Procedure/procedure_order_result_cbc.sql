/* CBC results. */
{%- set cbc_components = [
    ([5, 123050003, 123130024], 'hgb'),
    ([51, 123050000], 'wbc'),
    ([52, 123050002, 123050902], 'rbc'),
    ([53, 123050004], 'hct'),
    ([54, 123050005], 'mcv'),
    ([55, 123050006], 'mch'),
    ([56, 123050007], 'mchc'),
    ([57, 123050008], 'rdw'),
    ([58, 123050011], 'platelet_count'),
    ([59, 123050012], 'mpv')
] %}

with procedure_order_result_value as (
    select
        proc_ord_key,
        {%- for comp_id, comp_name in cbc_components %}
        max(
            case
                when
                    {%- for id in comp_id %}
                        result_component_id = {{ id }} {{ "OR" if not loop.last }}
                    {%- endfor %}
                    then result_value
                else null
            end
        ) as cbc_{{ comp_name }},
        max(
            case
                when
                    {%- for id in comp_id %}
                        result_component_id = {{ id }} {{ "OR" if not loop.last }}
                    {%- endfor %}
                    then result_value_numeric
                else null
            end
        ) as cbc_{{ comp_name }}_numeric{{ "," if not loop.last }}
        {%- endfor %}
    from
        {{ ref('procedure_order_result_clinical') }}
    where
        result_component_id in (
            {%- for comp_id, _ in cbc_components %}
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
    {%- for comp_id, comp_name in cbc_components %}
        cbc_{{ comp_name }},
        cbc_{{ comp_name }}_numeric{{ "," if not loop.last }}
    {%- endfor %}
from
    procedure_order_result_value
    left join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_result_value.proc_ord_key = procedure_order_clinical.proc_ord_key
