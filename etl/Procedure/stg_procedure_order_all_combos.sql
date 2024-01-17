with pb_transaction as (
    select
        stg_procedure_order_all_billing.visit_key,
        stg_procedure_order_all_billing.pat_key,
        stg_procedure_order_all_billing.proc_key,
        stg_procedure_order_all_billing.cpt_cd,
        stg_procedure_order_all_billing.billing_service_date,
        stg_procedure_order_all_billing.billing_service_date as join_date
    from
        {{ref('stg_procedure_order_all_billing')}} as stg_procedure_order_all_billing
    group by
        stg_procedure_order_all_billing.visit_key,
        stg_procedure_order_all_billing.pat_key,
        stg_procedure_order_all_billing.proc_key,
        stg_procedure_order_all_billing.cpt_cd,
        stg_procedure_order_all_billing.billing_service_date
),

clinical as (
    select
        stg_procedure_order_all_clinical.visit_key,
        stg_procedure_order_all_clinical.pat_key,
        stg_procedure_order_all_clinical.proc_key,
        stg_procedure_order_all_clinical.cpt_cd,
        null as billing_service_date,
        stg_procedure_order_all_clinical.encounter_date as join_date
    from
        {{ref('stg_procedure_order_all_clinical')}} as stg_procedure_order_all_clinical
    group by
        stg_procedure_order_all_clinical.visit_key,
        stg_procedure_order_all_clinical.pat_key,
        stg_procedure_order_all_clinical.proc_key,
        stg_procedure_order_all_clinical.cpt_cd,
        stg_procedure_order_all_clinical.encounter_date
)

select
    *,
    'pb transaction' as source_type
from
    pb_transaction
union all
select
    *,
    'clinical' as source_type
from
    clinical
