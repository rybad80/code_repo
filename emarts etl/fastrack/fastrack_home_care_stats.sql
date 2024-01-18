with tmp0 as (
select dphc.prod_num, dphc.prod_group, fshc.therapy, fshc.subcategory, fshc.discipline
from
{{ source('cdw', 'dim_product_hc') }} as dphc
left join {{ source('cdw', 'financial_statistics_control_hc') }} as fshc on dphc.dim_prod_key = fshc.dim_prod_key
)
, tmp as (
select 
fhc.match_hc_tx_id, p.pat_mrn_id as mrn, fhc.hc_tx_id as invoice, fhc.branch_num,
fhc.fc_key, fhc.rps_cd, fhc.sj_type as posting_type, fhc.svc_dt_key,
fhc.post_dt_key, fhc.retail_val, fhc.bill_type,
tmp0.therapy, tmp0.prod_group, tmp0.subcategory, tmp0.discipline,
fhc.billed_amt, fhc.proc_qty, fhc.amt, fhc.cost_cntr_key
from (
{{ source('cdw', 'fact_transaction_hc') }} as fhc
left join {{ source('cdw','patient') }} as p
    on fhc.pat_key = p.pat_key
)
left join  tmp0 on fhc.prod_num = tmp0.prod_num
)
, tmp1 as (
select tmp.match_hc_tx_id, tmp.mrn, tmp.invoice, tmp.branch_num, tmp.fc_key,
tmp.rps_cd, tmp.posting_type, tmp.svc_dt_key, tmp.post_dt_key, tmp.retail_val,
tmp.bill_type, tmp.therapy, tmp.prod_group, tmp.subcategory, tmp.discipline,
tmp.cost_cntr_key, tmp.billed_amt as total_billed_amt,
tmp.proc_qty as total_qty_delivered
from tmp
group by tmp.match_hc_tx_id, tmp.mrn, tmp.invoice, tmp.branch_num, tmp.fc_key, tmp.rps_cd, tmp.posting_type,
tmp.svc_dt_key, tmp.post_dt_key, tmp.retail_val, tmp.bill_type, tmp.therapy, tmp.prod_group, tmp.subcategory, tmp.discipline,
tmp.cost_cntr_key, tmp.billed_amt, tmp.proc_qty
) 
select tmp1.cost_cntr_key, tmp1.invoice, tmp1.branch_num, fc.fc_nm as insurance_group_code, tmp1.rps_cd, tmp1.posting_type,
tmp1.svc_dt_key, tmp1.post_dt_key, tmp1.retail_val, tmp1.bill_type, tmp1.therapy, tmp1.prod_group, tmp1.subcategory, tmp1.discipline,
tmp1.total_billed_amt, tmp1.total_qty_delivered,
case when (((((((tmp1.branch_num = '2') or (tmp1.branch_num = '4')) OR (tmp1.total_qty_delivered > '35'))
or (upper(fc.fc_nm) = 'SELF-PAY') ) or (upper(fc.fc_nm) = 'SELF PAY')) OR (fc.fc_nm ISNULL))
or (tmp1.bill_type = 'P')) then 0
when ((tmp1.rps_cd = 'T') OR ((tmp1.therapy = 'T') AND (tmp1.posting_type <> 'A'))) then tmp1.total_qty_delivered
else 0
end as therapy_days,
case when ((((tmp1.bill_type <> 'P') and ((tmp1.branch_num = '2') or (tmp1.branch_num = '4')))
and (tmp1.total_billed_amt > 0)) and (tmp1.rps_cd = 'R')) then tmp1.total_qty_delivered
else 0
end as rentals,
tmp1.match_hc_tx_id,
row_number() over 
    (partition by tmp1.cost_cntr_key, tmp1.branch_num, tmp1.svc_dt_key, 
    tmp1.post_dt_key, tmp1.bill_type, tmp1.match_hc_tx_id order by 'All') as sequence_nbr,
current_timestamp as update_date  
from tmp1
left join {{ source('cdw', 'financial_class') }} as fc on ((tmp1.fc_key = fc.fc_key))
