select
    pcoti_icu_transfers.icu_xfer_event_type_name,
    stg_patient.mrn,
    encounter_inpatient.csn,
    stg_patient.patient_name,
    stg_patient.dob,
    pcoti_icu_transfers.to_department_group_name,
    pcoti_icu_transfers.to_bed_care_group,
    pcoti_icu_transfers.to_campus_name,
    pcoti_icu_transfers.icu_enter_date,
    pcoti_icu_transfers.icu_exit_date,
    pcoti_icu_transfers.from_ip_service_name,
    pcoti_icu_transfers.from_department_name,
    pcoti_icu_transfers.from_department_group_name,
    pcoti_icu_transfers.from_bed_care_group,
    pcoti_icu_transfers.from_campus_name,
    case
        when pcoti_icu_transfers.gte_2hrs_in_icu_ind = 1 then 'Yes'
        else 'No'
    end as gte_2hrs_in_icu_ind,
    case
        when pcoti_icu_transfers.surg_proc_ind = 1 then 'Yes'
        else 'No'
    end as surg_proc_ind,
    pcoti_icu_transfers.surg_last_end_date,
    case
        when pcoti_icu_transfers.intubation_pre1hr_post1hr_ind = 1 then 'Yes'
        else 'No'
    end as intubation_pre1hr_post1hr_ind,
    pcoti_icu_transfers.intubation_first_start_date,
    case
        when pcoti_icu_transfers.vasopressor_pre1hr_post1hr_ind = 1 then 'Yes'
        else 'No'
    end as vasopressor_pre1hr_post1hr_ind,
    pcoti_icu_transfers.vasopressor_first_start_date,
    case
        when pcoti_icu_transfers.fluid_bolus_pre1hr_post1hr_ind = 1 then 'Yes'
        else 'No'
    end as fluid_bolus_pre1hr_post1hr_ind,
    pcoti_icu_transfers.fluid_bolus_gt60_date,
    case
        when pcoti_icu_transfers.cat_code_pre24hr_ind = 1 then 'Yes'
        else 'No'
    end as cat_code_pre24hr_ind,
    pcoti_icu_transfers.cat_code_first_date,
    case
        when pcoti_icu_transfers.unplanned_transfer_ind = 1 then 'Yes'
        else 'No'
    end as unplanned_transfer_ind
from
    {{ ref('pcoti_icu_transfers') }} as pcoti_icu_transfers
    inner join {{ ref('stg_patient') }} as stg_patient
        on pcoti_icu_transfers.pat_key = stg_patient.pat_key
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on pcoti_icu_transfers.visit_key = encounter_inpatient.visit_key
where
    pcoti_icu_transfers.prev_event_type_abbrev = 'LOC_ED'
    and pcoti_icu_transfers.surg_proc_ind = 0
