select
    stg_flowsheet_vitals_info.recorded_date,
    stg_flowsheet_vitals_info.flowsheet_record_id,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    visit.hosp_admit_dt as hospital_admit_date,
    visit.hosp_dischrg_dt as hospital_discharge_date,
    stg_flowsheet_vitals_info.weight_kg,
    stg_flowsheet_vitals_info.weight_change_kg,
    stg_flowsheet_vitals_info.dosing_weight_kg,
    stg_flowsheet_vitals_info.height_cm,
    stg_flowsheet_vitals_info.head_circumference_cm,
    stg_flowsheet_vitals_info.bmi,
    stg_flowsheet_vitals_info.bsa,
    stg_flowsheet_vitals_info.pulse,
    stg_flowsheet_vitals_info.spo2,
    stg_flowsheet_vitals_info.respiration,
    stg_flowsheet_vitals_info.temperature_c,
    stg_flowsheet_vitals_info.temperature_source,
    stg_flowsheet_vitals_info.secondary_temperature_c,
    stg_flowsheet_vitals_info.secondary_temperature_source,
    stg_flowsheet_vitals_info.systolic_blood_pressure_value,
    stg_flowsheet_vitals_info.diastolic_blood_pressure_value,
    case when (stg_flowsheet_vitals_info.blood_pressure_location_inpatient is not null
                and stg_flowsheet_vitals_info.blood_pressure_location_outpatient is not null)
        then (case when dict_enc_type.src_id = '3' --inpatient encounter
                then stg_flowsheet_vitals_info.blood_pressure_location_inpatient
                when dict_enc_type.src_id = '101' --outpatient encounter
                then stg_flowsheet_vitals_info.blood_pressure_location_outpatient
                end)
        when (stg_flowsheet_vitals_info.blood_pressure_location_outpatient is not null
                and stg_flowsheet_vitals_info.blood_pressure_location_inpatient is null)
        then stg_flowsheet_vitals_info.blood_pressure_location_outpatient
          else stg_flowsheet_vitals_info.blood_pressure_location_inpatient
    end as blood_pressure_location,
    stg_flowsheet_vitals_info.blood_pressure_cuff_size,
    stg_flowsheet_vitals_info.patient_position,
    stg_flowsheet_vitals_info.vsi_key,
    stg_patient.pat_key,
    stg_encounter.encounter_key,
    coalesce(anesthesia_encounter_link.visit_key, visit.visit_key) as visit_key,
    anesthesia_encounter_link.anes_visit_key
from
    {{source('cdw', 'visit')}} as visit
    inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
        on visit_stay_info.visit_key = visit.visit_key
    left join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
        on anesthesia_encounter_link.anes_visit_key = visit.visit_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = visit.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = visit.pat_key
    inner join {{ref('stg_flowsheet_vitals_info')}} as stg_flowsheet_vitals_info
        on visit_stay_info.vsi_key = stg_flowsheet_vitals_info.vsi_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_enc_type
        on dict_enc_type.dict_key = visit.dict_enc_type_key
where
    coalesce(anesthesia_encounter_link.visit_key, visit.visit_key) > 0
