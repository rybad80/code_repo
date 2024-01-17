with
enc_procedure_other_temp as (--for other procedures pull from procedure orders then find linked visits
   select distinct
        procedure_order_clinical.pat_key,
        procedure_order_clinical.mrn,
        procedure_order_clinical.visit_key,
        prov.full_nm as referring_prov_nm,
        appt_req_appt_links.pat_enc_csn_id as linked_visit_csn,
        poa.visit_key as linked_visit_key
    from {{ ref('stg_frontier_thyroid_cohort_base') }} as cohort_base
    inner join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on cohort_base.pat_key = procedure_order_clinical.pat_key
        and procedure_order_clinical.placed_date >= cohort_base.initial_date
        and year(add_months(procedure_order_clinical.placed_date, 6)) >= 2020
        and lower(procedure_order_clinical.order_status) != 'canceled'
    inner join {{ref('lookup_frontier_program_procedures')}} as lookup_fp_procedure
        on procedure_order_clinical.cpt_code = cast(lookup_fp_procedure.id as nvarchar(20))
        and lookup_fp_procedure.program = 'thyroid'
        and lower(lookup_fp_procedure.category) in ('ablation', 'ct', 'nm uptake and scan', 'ultrasound')
    inner join {{source('cdw', 'procedure_order')}} as po
        on po.proc_ord_key = procedure_order_clinical.proc_ord_key
    inner join {{source('cdw', 'provider')}} as prov
        on prov.prov_key = po.auth_prov_key
        and prov.prov_id in ('10352', --'bauer, andrew j.'
                            '2006317', --'robbins, stephanie l'
                            '5323', --'mostoufi moab, sogol',
                            '16489', --'kivel, courtney g',
                            '9810', --'laetsch, theodore',
                            '25535', --'meyers, kelly',
                            '2006349' --"o'reilly, stephanie  h"
                            )
    left join {{source('clarity_ods', 'appt_req_appt_links')}} as appt_req_appt_links
        on appt_req_appt_links.request_id = procedure_order_clinical.procedure_order_id
    left join {{source('cdw', 'procedure_order_appointment')}} as poa
        on poa.proc_ord_key = procedure_order_clinical.proc_ord_key
    where appt_req_appt_links.pat_enc_csn_id is not null or poa.visit_key is not null
),
enc_procedure_other as (
    select distinct procedure_billing.visit_key,
            procedure_billing.pat_key,
            procedure_billing.mrn,
            procedure_billing.service_date,
            lookup_fp_procedure.category
    from
        (select stg_encounter.visit_key
        from enc_procedure_other_temp
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on enc_procedure_other_temp.linked_visit_key = stg_encounter.visit_key
        union
        select stg_encounter.visit_key
        from enc_procedure_other_temp
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on enc_procedure_other_temp.linked_visit_csn = stg_encounter.csn
        ) as a
    inner join {{ ref('procedure_billing') }} as procedure_billing
        on a.visit_key = procedure_billing.visit_key
    inner join {{ref('lookup_frontier_program_procedures')}}  as lookup_fp_procedure
        on procedure_billing.cpt_code = cast(lookup_fp_procedure.id as nvarchar(20))
        and lookup_fp_procedure.program = 'thyroid'
        and lower(lookup_fp_procedure.category) in ('ablation', 'ct', 'nm uptake and scan', 'ultrasound')
)
--put all procedure encounters together including FNA and RAI
select enc_procedure_other.visit_key,
    0 as rai_visit_ind
from enc_procedure_other
left join {{ ref('stg_frontier_thyroid_enc_prcd_fna_rai') }} as enc_procedure_fna_rai
    on enc_procedure_fna_rai.pat_key = enc_procedure_other.pat_key
    and enc_procedure_fna_rai.category = 'fnab'
    and enc_procedure_fna_rai.service_date <= enc_procedure_other.service_date
where (enc_procedure_other.category = 'ablation' and enc_procedure_fna_rai.service_date is not null )
    or enc_procedure_other.category != 'ablation'
union
select visit_key,
    max(case when category = 'radioactive iodine' then 1 else 0 end) as rai_visit_ind
from {{ ref('stg_frontier_thyroid_enc_prcd_fna_rai') }}
group by visit_key
