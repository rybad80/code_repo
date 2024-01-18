select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    1 as surgical_bx_ind
from
    {{ ref('stg_procedure_order_all_billing') }} as stg_procedure_order_all_billing
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_procedure_order_all_billing.pat_enc_csn_id = stg_encounter.csn
where
    --thoractomy w/dx bx lung infiltrate unilateral (32096.*)
    --thoracoscopy w/dx bx of lung infiltrate unilatrl (32607.*)
    (regexp_like(lower(cpt_cd),
        '32096.*|
        |32607.*'
        ))
group by
    stg_encounter.visit_key,
    stg_encounter.mrn
