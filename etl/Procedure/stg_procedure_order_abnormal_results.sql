select
    procedure_order.proc_ord_key,
    min(order_status_hist.result_entry_dt) as abnormal_result_date
from
    {{source('cdw', 'procedure_order')}} as procedure_order
    inner join {{source('cdw', 'order_status_hist')}} as order_status_hist
        on order_status_hist.ord_key = procedure_order.proc_ord_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = procedure_order.visit_key
where
    order_status_hist.abnormal_result_ind = 1
group by
    procedure_order.proc_ord_key
