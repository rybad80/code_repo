-- engin consult orders placed by any providers
select
    procedure_order_clinical.proc_ord_key,
    procedure_order_clinical.pat_key,
    procedure_order_clinical.mrn,
    procedure_order_clinical.placed_date,
    procedure_order_clinical.visit_key,
    --procedure_order_clinical.ordering_provider_name,
    initcap(provider.full_nm) as referring_prov_nm,
    provider.prov_id as referring_prov_id
from
    {{ ref('procedure_order_clinical') }} as procedure_order_clinical
    inner join {{source('cdw', 'procedure_order') }} as procedure_order
        on procedure_order.proc_ord_key = procedure_order_clinical.proc_ord_key
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = procedure_order.auth_prov_key
where
    procedure_id = 119491 -- 'consult to epilepsy neurogenetics (chop)'
    and lower(procedure_order_clinical.order_status) != 'canceled'
    and year(add_months(procedure_order_clinical.placed_date, 6)) >= 2020
