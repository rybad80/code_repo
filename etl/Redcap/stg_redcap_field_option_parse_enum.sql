with distinct_elem_enum as (
    select
        redcap_metadata.project_id,
        redcap_metadata.form_name,
        redcap_metadata.field_name,
        redcap_metadata.element_type,
        redcap_metadata.element_enum,
        regexp_extract_all(
            replace(redcap_metadata.element_enum, '\n', chr(10)),
            '([\d-_\.]+, .*)'
        ) as element_enum_array,
        array_count(
            element_enum_array
        ) as num_elems,
        get_value_varchar(
            element_enum_array,
            lookup_redcap_enum_index.idx
        )::varchar(2000) as tuple,
        lookup_redcap_enum_index.idx as element_order,
        trim(regexp_extract(tuple, '^[\d-_\.]+(?=,)')::varchar(50))
            as element_id,
        trim(regexp_extract(tuple, '(?<=\d, ).+$')::varchar(2000))
            as element_desc
    from
        {{ source('ods_redcap_porter', 'redcap_metadata') }} as redcap_metadata
        cross join
            {{ ref('lookup_redcap_enum_index') }} as lookup_redcap_enum_index
    where
        redcap_metadata.element_type in ('select', 'checkbox', 'radio')
        and lookup_redcap_enum_index.idx <= num_elems
)

select
    project_id,
    form_name,
    field_name,
    element_enum as field_option_enum,
    element_order as field_option_id,
    element_id as field_option_value,
    element_desc as field_option_label
from
    distinct_elem_enum
