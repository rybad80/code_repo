with
stage as (
    select
        fact_transaction.tx_id,
        fact_transaction.visit_key,
        fact_transaction.dx1_key,
        fact_transaction.dx2_key,
        fact_transaction.dx3_key,
        fact_transaction.dx4_key,
        fact_transaction.dx5_key,
        fact_transaction.dx6_key,
        fact_transaction.pat_key,
        to_date(fact_transaction.chrg_svc_dt_key, 'yyyymmdd') as service_date,
        fact_transaction.proc_key,
        fact_transaction.svc_prov_key,
        fact_transaction.prov_specialty,
        place_of_service.pos_cd,
        fact_transaction.bill_prov_key,
        fact_transaction.dept_key,
        fact_transaction.mod1_key,
        fact_transaction.mod2_key,
        fact_transaction.mod3_key
    from
        {{source('cdw', 'fact_transaction')}} as fact_transaction
    inner join {{source('cdw', 'master_detail_type')}} as master_detail_type
        on fact_transaction.det_type_key = master_detail_type.det_type_key
    inner join {{ source('cdw', 'place_of_service')}} as place_of_service
        on fact_transaction.pos_key = place_of_service.pos_key
    where
        master_detail_type.det_type_id in (1, 10)
    group by
        fact_transaction.tx_id,
        fact_transaction.visit_key,
        fact_transaction.dx1_key,
        fact_transaction.dx2_key,
        fact_transaction.dx3_key,
        fact_transaction.dx4_key,
        fact_transaction.dx5_key,
        fact_transaction.dx6_key,
        fact_transaction.pat_key,
        fact_transaction.chrg_svc_dt_key,
        fact_transaction.proc_key,
        fact_transaction.svc_prov_key,
        fact_transaction.prov_specialty,
        place_of_service.pos_cd,
        fact_transaction.bill_prov_key,
        fact_transaction.dept_key,
        fact_transaction.mod1_key,
        fact_transaction.mod2_key,
        fact_transaction.mod3_key
    having
        sum(fact_transaction.proc_qty) > 0
)

select
    tx_id,
    visit_key,
    pat_key,
    service_date,
    proc_key,
    svc_prov_key,
    prov_specialty,
    pos_cd,
    bill_prov_key,
    dept_key,
    mod1_key,
    mod2_key,
    mod3_key,
    1 as dx_seq_num,
    dx1_key as dx_key,
    'physician billing' as source_summary
from
    stage
union all
select
    tx_id,
    visit_key,
    pat_key,
    service_date,
    proc_key,
    svc_prov_key,
    prov_specialty,
    pos_cd,
    bill_prov_key,
    dept_key,
    mod1_key,
    mod2_key,
    mod3_key,
    2 as dx_seq_num,
    dx2_key as dx_key,
    'physician billing' as source_summary
from
    stage

union all
select
    tx_id,
    visit_key,
    pat_key,
    service_date,
    proc_key,
    svc_prov_key,
    prov_specialty,
    pos_cd,
    bill_prov_key,
    dept_key,
    mod1_key,
    mod2_key,
    mod3_key,
    3 as dx_seq_num,
    dx3_key as dx_key,
    'physician billing' as source_summary

from stage

union all
select
    tx_id,
    visit_key,
    pat_key,
    service_date,
    proc_key,
    svc_prov_key,
    prov_specialty,
    pos_cd,
    bill_prov_key,
    dept_key,
    mod1_key,
    mod2_key,
    mod3_key,
    4 as dx_seq_num,
    dx4_key as dx_key,
    'physician billing' as source_summary

from stage

union all
select
    tx_id,
    visit_key,
    pat_key,
    service_date,
    proc_key,
    svc_prov_key,
    prov_specialty,
    pos_cd,
    bill_prov_key,
    dept_key,
    mod1_key,
    mod2_key,
    mod3_key,
    5 as dx_seq_num,
    dx5_key as dx_key,
    'physician billing' as source_summary

from stage

union all
select
    tx_id,
    visit_key,
    pat_key,
    service_date,
    proc_key,
    svc_prov_key,
    prov_specialty,
    pos_cd,
    bill_prov_key,
    dept_key,
    mod1_key,
    mod2_key,
    mod3_key,
    6 as dx_seq_num,
    dx6_key as dx_key,
    'physician billing' as source_summary
from stage
