/* Blood gas panel results. */
{%- set blood_gas_components = [
    ([24, 500611, 123030049, 123030341, 123030342, 123030349, 123130020], 'sample_site'),
    ([25], 'inspired_oxygen'),
    ([29, 123030073, 123130008], 'o2_tension'),
    ([30, 123030079, 123130009], 'bicarbonate_calc'),
    ([31, 4114, 501697, 502907, 502965, 123030081, 123030119, 123030158, 123130011, 123130051], 'base_excess'),
    ([32, 123030087], 'o2_sat'),
    ([33, 123030091], 'total_co2_calc'),
    ([35, 6649, 500688, 501684, 123030101, 123130003, 123130060], 'calculated_hematocrit'),
    ([141, 123130010], 'o2_sat_calc'),
    ([34, 123030093, 502549, 123130002, 123030146], 'total_hemoglobin'),
    ([127, 143, 123030021, 123130036], 'sodium_whole_bld'),
    ([128, 144, 123030022, 123130004], 'potassium_whole_bld'),
    ([145, 108, 500692, 123030013, 123130005], 'ionized_calcium')
] %}
{%- set ph_co2_components = [
    ([27, 4110, 123030070, 123030115, 123130006], 'ph_whole_bld'),
    ([28, 4111, 123030071, 123030116, 123130007], 'co2_tension')
] %}

with procedure_order_result_value as (
    select
        proc_ord_key,
        max(
            case
                when
                    result_component_id in (
                        123130020,
                        123030049,
                        123030341,
                        123030342,
                        123030349,
                        500611,
                        24)
                    then result_component_name
            end
        ) as sample_site_name,
        {% for comp_id, comp_name in (blood_gas_components + ph_co2_components) %}
            max(
                case
                    when
                        {%- for id in comp_id %}
                            result_component_id = {{ id }} {{ "or" if not loop.last -}}
                        {%- endfor %}
                        then result_value
                    else null
                end
            ) as blood_gas_{{ comp_name }},
            max(
                case
                    when
                        {%- for id in comp_id %}
                            result_component_id = {{ id }} {{ "or" if not loop.last -}}
                        {%- endfor %}
                        then result_value_numeric
                    else null
                end
            ) as blood_gas_{{ comp_name }}_numeric{{ "," if not loop.last }}   
        {%- endfor %}
    from
        {{ ref('procedure_order_result_clinical') }}
    where
        result_component_id in (
            {%- for comp_id, _ in (blood_gas_components + ph_co2_components) %}
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
    procedure_order_result_value.sample_site_name,
    {% for comp_id, comp_name in blood_gas_components %}
        blood_gas_{{ comp_name }},
        blood_gas_{{ comp_name }}_numeric{{ "," if not loop.last }}
    {%- endfor %},
    -- PH and CO2 iStat values need to be included into the venous columns
    -- when the sample site is "VEN". If it's art they go to the more general column.
    case
        when lower(blood_gas_sample_site) like 'ven%' then blood_gas_ph_whole_bld
    end as blood_gas_ph_whole_bld_venous,
    case
        when lower(blood_gas_sample_site) like 'ven%' then blood_gas_ph_whole_bld_numeric
    end as blood_gas_ph_whole_bld_venous_numeric,
    case
        when lower(blood_gas_sample_site) not like 'ven%' then blood_gas_ph_whole_bld
    end as blood_gas_ph_whole_bld,
    case
        when lower(blood_gas_sample_site) not like 'ven%' then blood_gas_ph_whole_bld_numeric
    end as blood_gas_ph_whole_bld_numeric,
    case
        when lower(blood_gas_sample_site) like 'ven%' then blood_gas_co2_tension
    end as blood_gas_pco2_tension_venous,
    case
        when lower(blood_gas_sample_site) like 'ven%' then blood_gas_co2_tension_numeric
    end as blood_gas_pco2_tension_venous_numeric,
    case
        when lower(blood_gas_sample_site) not like 'ven%' then blood_gas_co2_tension
    end as blood_gas_co2_tension,
    case
        when lower(blood_gas_sample_site) not like 'ven%' then blood_gas_co2_tension_numeric
    end as blood_gas_co2_tension_numeric,
    blood_gas_ph_whole_bld_numeric as blood_gas_ph_whole_bld_numeric_all,
    blood_gas_co2_tension_numeric as blood_gas_co2_tension_numeric_all,
    case
        when lower(procedure_order_result_value.blood_gas_sample_site) like '%art%'
        then 1 else 0
    end as blood_gas_arterial_ind,
    case
        when lower(procedure_order_result_value.blood_gas_sample_site) like '%ecmo%'
            or lower(procedure_order_result_value.blood_gas_sample_site) like '%lung%'
        then 1 else 0
    end as blood_gas_ecmo_ind,
    case
        when adt_department.department_group_name = 'SDU'
        then 1 else 0
    end as sdu_lab_ind
from
    procedure_order_result_value
    left join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_result_value.proc_ord_key = procedure_order_clinical.proc_ord_key
    left join {{ ref('adt_department') }} as adt_department
        on adt_department.visit_key = procedure_order_clinical.visit_key
            and procedure_order_clinical.specimen_taken_date >= adt_department.enter_date
            and procedure_order_clinical.specimen_taken_date < adt_department.exit_date
