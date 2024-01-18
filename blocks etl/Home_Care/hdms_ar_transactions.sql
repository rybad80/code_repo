with hdms_ar_trans as (
select
    ar.account,
    ar.medicalrecordnumber,
    ar.provideridentifier,
	ar.providername,
    ar.transdate,
    ar.transno,
    ar.transtype,
	ar.transtypedesc,
    COALESCE(ar.productcode, '') as productcode,
    UPPER(COALESCE(ar.billingcode, '')) as billingcode,
    COALESCE(ar.billingcodedesc, '') as billingcodedesc,
    ar.categorydescription,
    COALESCE(ar.itemtype, 0) as itemtype,
    ar.itemtypedesc,
    ar.payerindentifier as payeridentifier,
    SUM(ar.transamount) as transamount,
    SUM(ar.qty) as qty,
    case when ar.provideridentifier = 2
        and COALESCE(ar.itemtype, 0) = 2
        and UPPER(COALESCE(ar.billingcode, '')) != 'N0001'
        and SUM(ar.transamount) > 0
        then SUM(ar.qty)
    when ar.provideridentifier = 2
        and COALESCE(ar.itemtype, 0) = 2
        and UPPER(COALESCE(ar.billingcode, '')) != 'N0001'
        and SUM(ar.transamount) < 0
        then (SUM(ar.qty) * -1)
    else 0 end as rentals,
    case when ar.provideridentifier in (1, 3)
        and (
            UPPER(ar.categorydescription) = 'PROFESSIONAL SERVICES'
            or UPPER(COALESCE(ar.billingcodedesc, '')) like '%DIEM%'
            or UPPER(COALESCE(ar.billingcodedesc, '')) like '%TPN%'
            or UPPER(COALESCE(ar.billingcodedesc, '')) like '%HOME INFUS%'
            )
        and SUM(ar.transamount) > 0
        then SUM(ar.qty)
    when ar.provideridentifier in (1, 3)
        and (
            UPPER(ar.categorydescription) = 'PROFESSIONAL SERVICES'
            or UPPER(COALESCE(ar.billingcodedesc, '')) like '%DIEM%'
            or UPPER(COALESCE(ar.billingcodedesc, '')) like '%TPN%'
            or UPPER(COALESCE(ar.billingcodedesc, '')) like '%HOME INFUS%'
            )
        and SUM(ar.transamount) < 0
        then (SUM(ar.qty) * -1)
    else 0
    end as therapy_days
from {{ source('hdms_ods', 'hdms_ds_ar_transactiondetails') }} as ar
where
	ar.transtype = 1
	and ar.payerindentifier != 0
	and ar.transamount != 0
group by
    ar.account,
    ar.medicalrecordnumber,
    ar.provideridentifier,
	ar.providername,
    ar.transdate,
    ar.transno,
    ar.transtype,
	ar.transtypedesc,
    COALESCE(ar.productcode, ''),
    UPPER(COALESCE(ar.billingcode, '')),
    COALESCE(ar.billingcodedesc, ''),
    ar.categorydescription,
    COALESCE(ar.itemtype, 0),
    ar.itemtypedesc,
    ar.payerindentifier
)
select
hdms_ar_trans.account as hdms_account,
hdms_ar_trans.medicalrecordnumber as mrn,
stg_patient.pat_key,
hdms_ar_trans.provideridentifier as hdms_provider_identifier,
hdms_ar_trans.providername as hdms_provider_name,
hdms_ar_trans.transdate as hdms_transaction_date,
hdms_ar_trans.transno as hdms_transaction_nbr,
hdms_ar_trans.transtype as hdms_transaction_type,
hdms_ar_trans.transtypedesc as hdms_transaction_type_description,
hdms_ar_trans.productcode as hdms_product_code,
hdms_ar_trans.billingcode as hdms_billing_code,
hdms_ar_trans.billingcodedesc as hdms_billing_code_description,
hdms_ar_trans.categorydescription as hdms_category_description,
hdms_ar_trans.itemtype as hdms_item_type,
hdms_ar_trans.itemtypedesc as hdms_item_type_description,
hdms_ar_trans.payeridentifier as hdms_payer_identifier,
hdms_ds_tables_payers.payername as hdms_payor_name,
hdms_ds_tables_payers.glnumber as hdms_payor_glnumber,
hdms_ar_trans.transamount as transaction_amount,
hdms_ar_trans.qty as transaction_quantity,
hdms_ar_trans.rentals,
hdms_ar_trans.therapy_days,
CURRENT_TIMESTAMP as update_date
from hdms_ar_trans
left join {{ source('hdms_ods', 'hdms_ds_tables_payers') }} as hdms_ds_tables_payers
    on hdms_ar_trans.payeridentifier = hdms_ds_tables_payers.payeridentifier
left join {{ ref('stg_patient') }} as stg_patient
    on hdms_ar_trans.medicalrecordnumber = stg_patient.mrn
