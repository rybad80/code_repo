select distinct
    nephrology_encounter_dialysis.pat_key,
    nephrology_encounter_dialysis.patient_name,
    nephrology_encounter_dialysis.mrn,
    nephrology_encounter_dialysis.most_recent_dialysis_type,
    vaccination_all.grouper_records_numeric_id,
    received_date as immunization_date
from {{ref('nephrology_encounter_dialysis')}} as nephrology_encounter_dialysis
    inner join {{ ref('vaccination_all')}} as vaccination_all
        on vaccination_all.mrn = nephrology_encounter_dialysis.mrn
where
    (vaccination_all.grouper_records_numeric_id in (
    28, -- PNEUMOCOCCAL POLYSACCHARIDE
    89 -- PNEUMOCOCCAL 13 (PREVNAR 13)
    ) or influenza_vaccine_ind = 1) -- Flu vaccine
    and maintenance_dialysis_ind = 1
