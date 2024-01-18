with select_multiselect_fields as (
    select
            project_id,
            form_name,
            field_name,
            field_option_enum,
            field_option_id,
            field_option_value,
            field_option_label
    from
        {{ ref('stg_redcap_field_option_parse_enum') }}
),

all_other_fields as (
    select
        redcap_metadata.project_id,
        redcap_metadata.form_name,
        redcap_metadata.field_name,
        case
            when redcap_metadata.element_type = 'calc' then null
            else redcap_metadata.element_enum 
        end as field_option_enum,
        1 as field_option_id,
        null as field_option_value,
        null as field_option_label
    from
        {{ source('ods_redcap_porter', 'redcap_metadata') }} as redcap_metadata
    where
        element_type not in ('select', 'checkbox', 'radio')
),

fields_combined as (
    select 
        project_id,
        form_name,
        field_name,
        field_option_enum,
        field_option_id,
        field_option_value,
        field_option_label
    from 
        select_multiselect_fields

    union all

    select
        project_id,
        form_name,
        field_name,
        field_option_enum,
        field_option_id,
        field_option_value,
        field_option_label
    from 
        all_other_fields
),

record_union as (
    select 
        {{ dbt_utils.surrogate_key(
            [
                'project_id', 
                'form_name', 
                'field_name', 
                'field_option_id'
            ]
        ) }} as redcap_field_option_key,
        project_id,
        form_name,
        field_name,
        field_option_enum,
        field_option_id,
        field_option_value,
        field_option_label
    from
        fields_combined

    union all

    select 
        -1 as redcap_field_option_key,
        null as project_id,
        null as form_name,
        null as field_name,
        null as field_option_enum,
        null as field_option_id,
        'MISSING' as field_option_value,
        'MISSING' as field_option_label
)

select
    redcap_field_option_key,
    project_id,
    form_name,
    field_name,
    field_option_enum,
    field_option_id,
    field_option_value,
    field_option_label,
    'REDCAP~' || field_name || '~' || field_option_id as integration_id,
    current_timestamp as create_date,
    'REDCAP' as create_source,
    current_timestamp as update_date,
    'REDCAP' as update_source
from
    record_union
