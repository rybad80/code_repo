with field_records as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'redcap_metadata.project_id', 
                'redcap_metadata.form_name', 
                'redcap_metadata.field_name'
            ]
        ) }} as redcap_field_key,
        redcap_metadata.project_id,
        redcap_metadata.form_name,
        redcap_metadata.field_name,
        redcap_metadata.field_order,
        redcap_metadata.field_units,
        redcap_metadata.field_phi,
        redcap_metadata.element_type as field_type,
        redcap_metadata.element_label as field_label,
        redcap_metadata.element_note as field_note,
        redcap_metadata.element_validation_type as field_validation_type,
        redcap_metadata.element_validation_min as field_validation_min,
        redcap_metadata.element_validation_max as field_validation_max,
        redcap_metadata.element_validation_checktype
            as field_validation_checktype,
        case
            when element_type = 'calc' then element_enum
            else null
        end as field_calculation,
        redcap_metadata.branching_logic as field_branching_logic,
        redcap_metadata.field_req as required_field_ind
    from
        {{ source('ods_redcap_porter', 'redcap_metadata') }} as redcap_metadata
),

record_union as (
    select 
        redcap_field_key,
        project_id,
        form_name,
        field_name,
        field_order,
        field_units,
        field_phi,
        field_type,
        field_label,
        field_note,
        field_validation_type,
        field_validation_min,
        field_validation_max,
        field_validation_checktype,
        field_calculation,
        field_branching_logic,
        required_field_ind
    from 
        field_records

    union all

    select 
        -1 as redcap_field_key,
        null as project_id,
        null as form_name,
        'MISSING' as field_name,
        null as field_order,
        null as field_units,
        null as field_phi,
        null as field_type,
        null as field_label,
        null as field_note,
        null as field_validation_type,
        null as field_validation_min,
        null as field_validation_max,
        null as field_validation_checktype,
        null as field_calculation,
        null as field_branching_logic,
        null as required_field_ind
)

select
    redcap_field_key,
    project_id,
    form_name,
    field_name,
    field_order,
    field_units,
    field_phi,
    field_type,
    field_label,
    field_note,
    field_validation_type,
    field_validation_min,
    field_validation_max,
    field_validation_checktype,
    field_calculation,
    field_branching_logic,
    required_field_ind,
    'REDCAP~' || field_name as integration_id,
    current_timestamp as create_date,
    'REDCAP' as create_source,
    current_timestamp as update_date,
    'REDCAP' as update_source
from
    record_union
