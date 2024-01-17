select
    stg_procedure_order_clinical.visit_key,
    stg_procedure_order_clinical.pat_key,
    stg_procedure_order_clinical.cpt_cd,
    stg_procedure_order_clinical.proc_key,
    1 as procedure_order_ind,
    stg_encounter.encounter_date,
    stg_procedure_order_clinical.upd_dt
from
    {{ref('stg_procedure_order_clinical')}} as stg_procedure_order_clinical
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = stg_procedure_order_clinical.visit_key
where
    stg_procedure_order_clinical.proc_key != 0
group by
    stg_procedure_order_clinical.visit_key,
    stg_procedure_order_clinical.pat_key,
    stg_procedure_order_clinical.cpt_cd,
    stg_procedure_order_clinical.proc_key,
    stg_encounter.encounter_date,
    stg_procedure_order_clinical.upd_dt
