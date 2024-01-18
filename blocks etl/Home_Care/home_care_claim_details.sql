select
    hdms_ds_clm_claimlinedetails.claimnumber as hdms_claim_number,
    hdms_ds_clm_claimlinedetails.claimlineidentifier as hdms_claim_line_id,
    hdms_ds_clm_claimlinedetails.account as hdms_account,
    hdms_ds_patient_demographics.patientmedicalrecordnumber as mrn,
    hdms_ds_clm_claimlinedetails.patientfullname as hdms_patient_name,
    hdms_ds_clm_claimlinedetails.productcode as hdms_product_code,
    hdms_ds_clm_claimlinedetails.productdescription as hdms_product_description,
    hdms_ds_clm_claimlinedetails.productcategorydescription as hdms_category_description,
    hdms_ds_clm_claimlinedetails.deliverymethod as hdms_delivery_method,
    hdms_ds_clm_claimlinedetails.dateofservice as hdms_date_of_service,
    hdms_ds_clm_claimlinedetails.dateofserviceage as hdms_dos_age,
    hdms_ds_clm_claimlinedetails.billingcode as hdms_billing_code,
    hdms_ds_clm_claimlinedetails.hcpc as hdms_hcpc,
    hdms_ds_clm_claimlinedetails.billingcodedescription as hdms_billing_code_description,
    hdms_ds_clm_claimlinedetails.itemtypedesc as hdms_item_type_description,
    hdms_ds_clm_claimlinedetails.payeridentification as hdms_payer_identifier,
    hdms_ds_clm_claimlinedetails.payername as hdms_payor_name,
    hdms_ds_clm_claimlinedetails.payertypename as hdms_payor_type,
    hdms_ds_clm_claimlinedetails.unbilledreason as hdms_unbilled_reason,
    hdms_ds_clm_claimlinedetails.grosscharge as hdms_gross_charge,
    hdms_ds_clm_claimlinedetails.netcharge as hdms_net_charge,
    hdms_ds_clm_claimlinedetails.providername as hdms_provider_name,
    hdms_ds_clm_claimlinedetails.provideridentifier as hdms_provider_identifier,
    case
        when
            patientstatuscode1 = '' then ''
        else substr(patientstatuscode1, 0, 3)
    end
    || case when patientstatuscode2 = '' then '' else ', ' end
    || case
        when
            patientstatuscode2 = '' then ''
        else substr(patientstatuscode2, 0, 3)
    end
    || case when patientstatuscode3 = '' then '' else ', ' end
    || case
        when
            patientstatuscode3 = '' then ''
        else substr(patientstatuscode3, 0, 3)
    end
    || case when patientstatuscode4 = '' then '' else ', ' end
    || case
        when
            patientstatuscode4 = '' then ''
        else substr(patientstatuscode4, 0, 3)
    end
    || case when patientstatuscode5 = '' then '' else ', ' end
    || case
        when
            patientstatuscode5 = '' then ''
        else substr(patientstatuscode5, 0, 3)
    end
    || case when patientstatuscode6 = '' then '' else ', ' end
    || case
        when
            patientstatuscode6 = '' then ''
        else substr(patientstatuscode6, 0, 3)
    end
    || case when patientstatuscode7 = '' then '' else ', ' end
    || case
        when
            patientstatuscode7 = '' then ''
        else substr(patientstatuscode7, 0, 3)
    end
    || case when patientstatuscode8 = '' then '' else ', ' end
    || case
        when
            patientstatuscode8 = '' then ''
        else substr(patientstatuscode8, 0, 3)
    end
    || case when patientstatuscode9 = '' then '' else ', ' end
    || case
        when
            patientstatuscode9 = '' then ''
        else substr(patientstatuscode9, 0, 3)
    end
    || case when patientstatuscode10 = '' then '' else ', ' end
    || case
        when
            patientstatuscode10 = '' then ''
        else substr(patientstatuscode10, 0, 3)
    end as patient_status_codes

from
    {{ source('hdms_ods', 'hdms_ds_clm_claimlinedetails') }} as hdms_ds_clm_claimlinedetails
left join {{
    source('hdms_ods', 'hdms_ds_patient_demographics') }} as hdms_ds_patient_demographics on
    hdms_ds_clm_claimlinedetails.account = hdms_ds_patient_demographics.account
