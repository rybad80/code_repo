{{ config(meta = {
    'critical': true
}) }}

select
    or_xref.or_key,
    or_xref.or_id,
    cdw_dictionary.dict_cat_nm,
    cdw_dictionary.src_id,
    cdw_dictionary.dict_nm,
    or_xref.create_by as source_system
from
    {{ source('cdw', 'or_xref') }} as or_xref
    inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
        on or_xref.dict_or_type_key = cdw_dictionary.dict_key
where
    cdw_dictionary.dict_cat_nm = 'OR_TYPE'
