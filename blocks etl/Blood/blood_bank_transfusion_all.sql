{{
    config(
        materialized = 'view',
        meta = {
        'critical': true
        }
    )
}}
select
    fact_blood_bank_transfusion.blood_bank_transfusion_key,
    fact_blood_bank_transfusion.integration_id,
    fact_blood_bank_transfusion.product_inventory_id,
    fact_blood_bank_transfusion.unit_no,
    fact_blood_bank_transfusion.division,
    fact_blood_bank_transfusion.product_id,
    fact_blood_bank_transfusion.product_name,
    fact_blood_bank_transfusion.ecode,
    fact_blood_bank_transfusion.ecode_description,
    fact_blood_bank_transfusion.available_quantity,
    fact_blood_bank_transfusion.product_abo_type,
    fact_blood_bank_transfusion.product_rh_factor,
    fact_blood_bank_transfusion.aliquot_ind,
    fact_blood_bank_transfusion.irradiated_ind,
    fact_blood_bank_transfusion.washed_ind,
    fact_blood_bank_transfusion.product_preservative,
    fact_blood_bank_transfusion.crossmatch_test_id,
    fact_blood_bank_transfusion.crossmatch_test_name,
    fact_blood_bank_transfusion.crossmatch_test_result,
    fact_blood_bank_transfusion.crossmatch_test_result_details,
    fact_blood_bank_transfusion.crossmatch_test_specimen_id,
    fact_blood_bank_transfusion.crossmatch_test_specimen_number,
    fact_blood_bank_transfusion.patient_key,
    fact_blood_bank_transfusion.blood_bank_patient_id,
    stg_patient_ods.pat_id,
    stg_patient_ods.mrn,
    stg_patient_ods.dob,
    round(months_between(fact_blood_bank_transfusion.issue_datetime,
        stg_patient_ods.dob), 2) as age_in_months_at_issue,
    stg_patient_ods.sex,
    dim_blood_bank_patient.abo_type as patient_abo_type,
    dim_blood_bank_patient.rh_factor as patient_rh_factor,
    fact_blood_bank_transfusion.procedure_order_id,
    fact_blood_bank_transfusion.product_location_id,
    fact_blood_bank_transfusion.order_sublocation_id,
    fact_blood_bank_transfusion.provider_id,
    fact_blood_bank_transfusion.blood_bank_provider_id,
    fact_blood_bank_transfusion.provider_name,
    fact_blood_bank_transfusion.order_datetime,
    fact_blood_bank_transfusion.issue_datetime,
    fact_blood_bank_transfusion.transfusion_start_datetime,
    fact_blood_bank_transfusion.transfusion_end_datetime,
    fact_blood_bank_transfusion.product_draw_date,
    fact_blood_bank_transfusion.product_expiration_datetime
from {{ref('fact_blood_bank_transfusion')}} as fact_blood_bank_transfusion
inner join {{ref('dim_blood_bank_patient')}} as dim_blood_bank_patient
    on dim_blood_bank_patient.patient_key = fact_blood_bank_transfusion.patient_key
left join {{ref('stg_patient_ods')}} as stg_patient_ods
    on stg_patient_ods.patient_key = fact_blood_bank_transfusion.patient_key
