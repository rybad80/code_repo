with
eeg_enc_temp as ( --include eeg procedure visits
   select distinct
        procedure_order_clinical.pat_key,
        procedure_order_clinical.mrn,
        procedure_order_clinical.visit_key,
        --prov.full_nm as referring_prov_nm,
        appt_req_appt_links.pat_enc_csn_id as linked_visit_csn,
        poa.visit_key as linked_visit_key
    from {{ ref('stg_frontier_engin_cohort_base') }} as cohort_base
    inner join  {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on cohort_base.pat_key = procedure_order_clinical.pat_key
        and procedure_order_clinical.placed_date >= cohort_base.engin_start_date
        and year(add_months(procedure_order_clinical.placed_date, 6)) >= 2020
        and lower(procedure_order_clinical.order_status) != 'canceled'
        and (regexp_like(lower(procedure_order_clinical.procedure_name), '\beeg\b')
             or regexp_like(lower(procedure_order_clinical.procedure_name), '\belectroencephalograph\b'))
    inner join {{source('cdw', 'procedure_order')}} as po
        on po.proc_ord_key = procedure_order_clinical.proc_ord_key
    inner join {{source('cdw', 'provider')}} as prov
        on prov.prov_key = po.auth_prov_key
    inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_fp_providers
        on prov.prov_id = cast(lookup_fp_providers.provider_id as nvarchar(20))
        and lookup_fp_providers.program = 'engin'
        and lookup_fp_providers.provider_type = 'general'
        and lookup_fp_providers.active_ind = 1
    left join  {{source('clarity_ods', 'appt_req_appt_links')}} as appt_req_appt_links
        on appt_req_appt_links.request_id = procedure_order_clinical.procedure_order_id
    left join  {{source('cdw', 'procedure_order_appointment')}} as poa
        on poa.proc_ord_key = procedure_order_clinical.proc_ord_key
    where appt_req_appt_links.pat_enc_csn_id is not null or poa.visit_key is not null
)
-- filter with billing table because some appt may not really happen
select distinct
    procedure_billing.visit_key,
    procedure_billing.pat_key,
    procedure_billing.mrn
from
    (select stg_encounter.visit_key
    from eeg_enc_temp
    inner join  {{ ref('stg_encounter') }} as stg_encounter
        on eeg_enc_temp.linked_visit_key = stg_encounter.visit_key
    union
    select stg_encounter.visit_key
    from eeg_enc_temp
    inner join  {{ ref('stg_encounter') }} as stg_encounter
        on eeg_enc_temp.linked_visit_csn = stg_encounter.csn
    ) as a
inner join {{ ref('procedure_billing') }} as procedure_billing
    on a.visit_key = procedure_billing.visit_key
    and (regexp_like(lower(procedure_billing.procedure_name), '\beeg\b')
            or regexp_like(lower(procedure_billing.procedure_name), '\belectroencephalograph\b') )
