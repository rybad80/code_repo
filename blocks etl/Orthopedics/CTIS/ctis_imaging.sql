select
    procedure_order_clinical.proc_ord_key,
    procedure_order_clinical.mrn,
    procedure_order_clinical.procedure_name,
    procedure_order_clinical.procedure_id,
    procedure_order_clinical.cpt_code,
    procedure_order_clinical.placed_date,
    coalesce(procedure_order_clinical.placed_date,
        procedure_order_clinical.specimen_taken_date) as event_date,
    procedure_order_clinical.order_class,
    procedure_order_clinical.procedure_order_type,
    master_date.c_yyyy as calendar_year,
    master_date.f_yyyy as fiscal_year,
    master_date.fy_yyyy_qtr as fiscal_quarter,
    procedure_order_clinical.pat_key,
    procedure_order_clinical.visit_key
from {{ ref('ctis_registry') }} as ctis_registry
    inner join {{ ref('procedure_order_clinical')}} as procedure_order_clinical
        on ctis_registry.pat_key = procedure_order_clinical.pat_key
    inner join {{ source('cdw', 'procedure_order_narrative') }} as procedure_order_narrative
        on procedure_order_narrative.proc_ord_key = procedure_order_clinical.proc_ord_key
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.full_dt = date(coalesce(
            procedure_order_clinical.placed_date, procedure_order_clinical.specimen_taken_date))
    inner join {{ ref('lookup_frontier_program_procedures')}} as lookup_frontier_program_procedures
        on procedure_order_clinical.procedure_id = cast(
                lookup_frontier_program_procedures.id as nvarchar(20))
        and lookup_frontier_program_procedures.program = 'ctis'
        and lookup_frontier_program_procedures.category = 'imaging'
        and lookup_frontier_program_procedures.active_ind = 1
where
    procedure_order_narrative.seq_num = 1
