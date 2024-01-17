select
    medication_order_administration.visit_key,
    medication_order_administration.med_ord_key,
    medication_order_administration.patient_name,
    medication_order_administration.mrn,
    medication_order_administration.encounter_date,
    medication_order_administration.hospital_admit_date,
    medication_order_administration.hospital_discharge_date,
    medication_order_administration.authorizing_prov_key,
     case when
            medication_order_administration.authorizing_provider_name = 'HISTORICAL MEDICATION' then
            stg_encounter.provider_name
        else medication_order_administration.authorizing_provider_name
    end as authorizing_provider_name,
    stg_encounter.provider_name as encounter_provider_name,
    medication_order_administration.ordering_provider_name,
    medication_order_administration.medication_name,
    medication_order_administration.generic_medication_name,
    medication_order_administration.medication_id,
    medication_order_administration.therapeutic_class,
    medication_order_administration.pharmacy_class,
    medication_order_administration.medication_start_date,
    medication_order_administration.medication_end_date,
    medication_order_administration.order_status,
    medication_order_administration.order_mode,
    medication_order_administration.order_class,
    medication_order_administration.discharge_med_ind,
    medication_order_administration.ordering_department,
    medication_order_administration.administration_date,
    medication_order_administration.administration_department,
    medication_order_administration.pat_key
from
    {{ref('medication_order_administration')}} as medication_order_administration
    inner join {{source('clarity_ods', 'grouper_compiled_rec_list')}} as grouper_compiled_rec_list
        on medication_order_administration.medication_id = grouper_compiled_rec_list.grouper_records_numeric_id
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = medication_order_administration.visit_key
where
    grouper_compiled_rec_list.base_grouper_id = '114991' --  CHOP HP BH REGISTRY MEDS 
    and medication_order_administration.encounter_date >= '2018-01-01'
    and medication_order_administration.order_status != 'Canceled'
