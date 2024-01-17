select
    fact_redcap_record_value.redcap_record_value_key,
    fact_redcap_record_value.redcap_project_key,
    fact_redcap_record_value.project_id,
    fact_redcap_record_value.redcap_event_key,
    fact_redcap_record_value.event_id,
    dim_redcap_event.event_name,
    dim_redcap_event.arm_id,
    dim_redcap_event.arm_num,
    dim_redcap_event.arm_name,
    coalesce(
        dim_redcap_form.redcap_form_key,
        -1
    ) as redcap_form_key,
    dim_redcap_form.form_name,
    dim_redcap_form.form_menu_description,
    coalesce(
        dim_redcap_field.redcap_field_key,
        -1
    ) as redcap_field_key,
    fact_redcap_record_value.field_name,
    dim_redcap_field.field_order,
    dim_redcap_field.field_label,
    dim_redcap_field.field_type,
    dim_redcap_field.field_calculation,
    fact_redcap_record_value.record_id,
    fact_redcap_record_value.record_instance_num,
    fact_redcap_record_value.record_value,
    coalesce(
        dim_redcap_field_option.redcap_field_option_key,
        -1
    ) as redcap_field_option_key,
    dim_redcap_field_option.field_option_value,
    dim_redcap_field_option.field_option_label,
    case
        when dim_redcap_field.field_type in ('checkbox', 'select', 'radio')
        and dim_redcap_field_option.field_option_enum is not null
        then fact_redcap_record_value.field_name
            || '__'
            || dim_redcap_field_option.field_option_id
        else fact_redcap_record_value.field_name
    end as compound_field_name
from
    {{ ref('fact_redcap_record_value') }} as fact_redcap_record_value
    left join {{ ref('dim_redcap_event') }} as dim_redcap_event
        on fact_redcap_record_value.event_id = dim_redcap_event.event_id
    left join {{ ref('dim_redcap_field') }} as dim_redcap_field
        on fact_redcap_record_value.field_name = dim_redcap_field.field_name
        and fact_redcap_record_value.project_id = dim_redcap_field.project_id
    left join {{ ref('dim_redcap_form') }} as dim_redcap_form
        on dim_redcap_field.form_name = dim_redcap_form.form_name
        and fact_redcap_record_value.project_id = dim_redcap_form.project_id
    left join {{ ref('dim_redcap_field_option') }} as dim_redcap_field_option
        on
            fact_redcap_record_value.project_id
            = dim_redcap_field_option.project_id
        and dim_redcap_field.form_name = dim_redcap_field_option.form_name
        and fact_redcap_record_value.field_name
        = dim_redcap_field_option.field_name
        and fact_redcap_record_value.record_value
        = dim_redcap_field_option.field_option_value
