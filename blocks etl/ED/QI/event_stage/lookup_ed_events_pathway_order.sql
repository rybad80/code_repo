select distinct
    'pathway_ordered' as event_category,
    regexp_replace(
        regexp_replace(
            trim(procedure_name), ' ', '_'),
            '/', '_'
    ) as event_name,
    procedure_id,
    procedure_name as description
from
    {{ ref('procedure_order_clinical') }}
where
    lower(procedure_name) like 'ed%pathway%'
        or procedure_id in (
        97345 --'ED NON-ONCOLOGY PATIENTS WITH A CENTRAL VENOUS CATHETER AND FEVER'
    )
