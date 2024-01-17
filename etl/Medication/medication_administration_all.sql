select
    fact_medication_administration.medication_administration_key,
    -- demographics
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date,
    -- about med
    dim_medication.medication_name,
    dim_medication.medication_id,
    dim_medication.medication_form,
    dim_medication.medication_strength,
    dim_medication.generic_medication_name,
    dim_medication.generic_product_identifier,
    dim_medication.therapeutic_class,
    dim_medication.therapeutic_class_id,
    dim_medication.pharmacy_class,
    dim_medication.pharmacy_class_id,
    dim_medication.pharmacy_sub_class,
    dim_medication.pharmacy_sub_class_id,
    dim_medication.specialty_medication_ind,
    --admin
    fact_medication_administration.medication_order_id,
    fact_medication_administration.admin_seq_number,
    fact_medication_administration.admin_result,
    fact_medication_administration.admin_result_id,
    fact_medication_administration.given_ind,
    fact_medication_administration.first_given_ind,
    fact_medication_administration.admin_date,
    fact_medication_administration.admin_dose,
    fact_medication_administration.admin_dose_unit,
    fact_medication_administration.admin_infusion_rate,
    fact_medication_administration.admin_route,
    fact_medication_administration.admin_route_group,
    fact_medication_administration.admin_department,
    fact_medication_administration.admin_department_id,
    -- keys
    fact_medication_order.medication_order_key,
    fact_medication_order.medication_key,
    stg_patient.pat_key,
    stg_patient.patient_key,
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    -- bioresponse detail
    lookup_bioresponse_medication.diagnosis_hierarchy_1 as bioresponse_related_infection
from
    {{ref('fact_medication_administration')}} as fact_medication_administration
    inner join {{ref('fact_medication_order')}} as fact_medication_order
        on fact_medication_order.medication_order_key = fact_medication_administration.medication_order_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = fact_medication_order.csn
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = stg_encounter.pat_key
    inner join {{ref('dim_medication')}} as dim_medication
        on dim_medication.medication_key = fact_medication_order.medication_key
    left join {{ ref('lookup_bioresponse_medication') }} as lookup_bioresponse_medication
        on cast(lookup_bioresponse_medication.medication_id as varchar(100)) = dim_medication.medication_id
where
    {{ limit_dates_for_dev(ref_date = 'fact_medication_administration.admin_date') }}
