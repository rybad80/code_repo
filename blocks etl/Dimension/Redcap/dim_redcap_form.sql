with form_records as (
    select
        {{ dbt_utils.surrogate_key(['redcap_metadata.project_id', 'redcap_metadata.form_name']) }}
            as redcap_form_key,
        redcap_metadata.project_id,
        redcap_metadata.form_name,
        max(redcap_metadata.form_menu_description) as form_menu_description
    from
        {{ source('ods_redcap_porter', 'redcap_metadata') }} as redcap_metadata
    group by
        redcap_metadata.project_id,
        redcap_metadata.form_name
),

record_union as (
    select
        redcap_form_key,
        project_id,
        form_name,
        form_menu_description
    from
        form_records

    union all

    select 
        -1 as redcap_form_key,
        null as project_id,
        'MISSING' as form_name,
        null as form_menu_description
)

select
    redcap_form_key,
    project_id,
    form_name,
    form_menu_description,
    'REDCAP~' || form_name as integration_id,
    current_timestamp as create_date,
    'REDCAP' as create_source,
    current_timestamp as update_date,
    'REDCAP' as update_source
from
    record_union
