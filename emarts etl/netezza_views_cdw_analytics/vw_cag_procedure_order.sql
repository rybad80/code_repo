select
    pat.pat_mrn_id as mrn,
    v.enc_id as encounter_id,
    md.full_dt as procedure_order_date,
    proc.proc_nm as procedure_name,
    po.cpt_cd,
    proc.proc_cd as procedure_code,
    dict1.dict_nm as order_type,
    po.proc_ord_id as procedure_order_id
from
    {{source('cdw', 'procedure_order')}} as po
    inner join {{source('cdw', 'patient')}} as pat on ((po.pat_key = pat.pat_key))
    inner join {{source('cdw', 'visit')}} as v on ((po.visit_key = v.visit_key))
    inner join {{source('cdw', 'master_date')}} as md on ((po.proc_ord_dt_key = md.dt_key))
    inner join {{source('cdw', 'cdw_dictionary')}} as dict1 on ((po.dict_ord_type_key = dict1.dict_key))
    inner join {{source('cdw', 'procedure')}} as proc on ((po.proc_key = proc.proc_key))
    inner join {{source('cdw_customer', 'cohort_cag_mrn')}} as cag_mrn on ((cag_mrn.mrn = pat.pat_mrn_id))
