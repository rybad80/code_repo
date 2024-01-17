select
    fact_transaction_hb.pat_key,
    fact_transaction_hb.tx_id,
    fact_transaction_hb.visit_key,
    to_date(fact_transaction_hb.svc_dt_key, 'yyyymmdd') as service_date,
    fact_transaction_hb.proc_key,
    null as dx_key,
    -1 as dx_seq_num,
    fact_transaction_hb.svc_prov_key,
    null as prov_specialty,
    null as pos_cd,
    fact_transaction_hb.bill_prov_key,
    fact_transaction_hb.dept_key,
    fact_transaction_hb.mod1_key,
    fact_transaction_hb.mod2_key,
    fact_transaction_hb.mod3_key,
    'hospital billing' as source_summary
from
    {{source('cdw', 'fact_transaction_hb')}} as fact_transaction_hb
where
    fact_transaction_hb.svc_dt_key != -1
