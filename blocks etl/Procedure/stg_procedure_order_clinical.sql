select
    procedure_order.visit_key,
    procedure_order.pat_key,
    procedure_order.proc_key,
    procedure.cpt_cd,
    max(procedure_order.upd_dt) as upd_dt
from
    {{source('cdw', 'procedure_order')}} as procedure_order
    inner join {{source('cdw', 'procedure')}} as procedure --noqa: L029
        on procedure.proc_key = procedure_order.proc_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = procedure_order.visit_key
group by
    procedure_order.visit_key,
    procedure_order.pat_key,
    procedure_order.proc_key,
    procedure.cpt_cd
