{#
this data currently comes from clarity 'order_med'. The legacy medication tables also have data from
SCM  (Sunrise Clinical Manager). If we decide to integrate the SCM data, this logic should be put into a stage table
and a similar table should be added for SCM and then exposed through this fact table with a union
#}


select
    fact_medication_order.medication_order_key,
    --demographics
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    fact_medication_order.csn,
    stg_encounter.encounter_date,
    -- order details
    fact_medication_order.medication_order_id,
    fact_medication_order.medication_order_create_date,
    fact_medication_order.medication_order_name,
    fact_medication_order.medication_start_date,
    fact_medication_order.medication_end_date,
    -- about med
    dim_medication.medication_name,  -- ??? do we need all of these
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
    -- more about admin
    fact_medication_order.order_dose,
    fact_medication_order.order_dose_unit,
    fact_medication_order.order_route,
    fact_medication_order.order_route_group,
    fact_medication_order.formulary_med_ind,
    fact_medication_order.quantity,
    fact_medication_order.n_refills_allowed,
    fact_medication_order.n_refills_remaining,
    fact_medication_order.pharmacy_id,
    fact_medication_order.pharmacy_name,
    fact_medication_order.order_frequency,
    fact_medication_order.order_mode,
    fact_medication_order.order_class,
    fact_medication_order.historical_med_ind,
    fact_medication_order.control_med_ind, -- ??? what about from dim_medication table
    fact_medication_order.dea_code_id,
    fact_medication_order.dea_class_code,
    fact_medication_order.active_order_status,
    fact_medication_order.pending_ind,
    fact_medication_order.scheduled_start_time,
    fact_medication_order.discontinue_date,
    case
        when
            fact_medication_order.order_mode_id = 1
            and (
                fact_medication_order.discontinue_date is null
                or timezone(fact_medication_order.discontinue_date, 'UTC', 'America/New_York')
                   > coalesce(stg_encounter.hospital_discharge_date, fact_medication_order.medication_start_date)
            )
            and (
                stg_encounter.hospital_discharge_date is not null
                or stg_encounter.encounter_type != 'HOSPITAL ENCOUNTER'
            )
        then 1
        else 0
        end as discharge_med_ind,
    fact_medication_order.orderset_id,
    fact_medication_order.orderset_name,
    fact_medication_order.ordering_provider_id,
    fact_medication_order.ordering_provider_name,
    fact_medication_order.authorizing_provider_id,
    fact_medication_order.authorizing_provider_name,
    fact_medication_order.order_comments,
    fact_medication_order.patient_department,
    fact_medication_order.patient_department_id,
    fact_medication_order.weight_based_ind,
    -- keys
    stg_patient.pat_key,
    stg_encounter.visit_key,
    fact_medication_order.medication_key
from
    {{ref('fact_medication_order')}} as fact_medication_order
    inner join {{ref('dim_medication')}} as dim_medication
        on dim_medication.medication_key = fact_medication_order.medication_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = fact_medication_order.csn
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_encounter.pat_key = stg_patient.pat_key
