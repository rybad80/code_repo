with missing_cath_orders as (
select 
     refno,
     patid as mrn,
     studate,
     admissid,
     accessno,
     reqprid,
     ordnum  
from cdw_ods..sensis_study 
      join cdw_ods..sensis_patient on sensis_study.patno = sensis_patient.patno
where
    ordnum is null
    and studate > '2021-06-01'
    and reqprid is not null
),

cath_orders as (
select  
      mrn,
      placed_date,
      result_date,
      cast(procedure_order_id as integer) as order_number
      
from 
    chop_analytics..procedure_order_clinical
where 
    lower(procedure_name) = 'cath lab procedure'
    and lower(procedure_order_type) = 'child order'
  
)

select 
      refno as sensis_refno,
      missing_cath_orders.mrn,
      studate as cath_date,
      admissid as admission_csn,
      accessno as accession_number,
      reqprid as sensis_case_number,
      ordnum as sensis_order_number,
      cath_orders.order_number as epic_order_number
 from 
     missing_cath_orders 
     left join cath_orders on missing_cath_orders.mrn = cath_orders.mrn and missing_cath_orders.studate = date(cath_orders.result_date)
where
     missing_cath_orders.mrn =  '56531877'