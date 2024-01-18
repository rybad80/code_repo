select
    cohort.log_key,
    cohort.visit_key,
    cohort.mrn,
    cohort.patient_name,
    cohort.surgery_date,
    cohort.first_panel_first_procedure_name,
    cohort.room,
    cohort.leave_nicu_time,
    cohort.in_preop_room_date,
    cohort.in_room_date,
    cohort.anesthesia_start_date,
    cohort.procedure_start_date,
    cohort.procedure_close_date,
    cohort.anesthesia_stop_date,
    cohort.out_room_date,
    cohort.recovery_exit_date,
    cohort.return_nicu_time,
    cohort.admitted_after_surgery_ind,
    cohort.stepp_ind,
    stg_nicu_chnd_flowsheets.surgery_weight,
    stg_nicu_chnd_flowsheets.pre_temp_value,
    stg_nicu_chnd_flowsheets.post_temp_value,
    stg_nicu_chnd_flowsheets.post_temp_source,
    stg_nicu_chnd_flowsheets.post_rectal_temp_ind,
    stg_nicu_chnd_procedures.pre_pco2_value,
    stg_nicu_chnd_procedures.pre_ph_value,
    stg_nicu_chnd_procedures.pre_glucose_value,
    stg_nicu_chnd_procedures.post_pco2_value,
    stg_nicu_chnd_procedures.post_ph_value,
    stg_nicu_chnd_procedures.post_glucose_value,
    stg_nicu_chnd_blood.intraop_platelets_volume,
    stg_nicu_chnd_blood.intraop_cryo_volume,
    stg_nicu_chnd_blood.intraop_prbc_volume,
    stg_nicu_chnd_blood.intraop_ffp_volume,
    stg_nicu_chnd_blood.postop_platelets_volume,
    stg_nicu_chnd_blood.postop_cryo_volume,
    stg_nicu_chnd_blood.postop_prbc_volume,
    stg_nicu_chnd_blood.postop_ffp_volume,
    stg_nicu_chnd_medications.anesthesia_med,
    stg_nicu_chnd_medications.total_dose
from
    {{ ref('stg_nicu_chnd_timestamps') }} as cohort
    inner join {{ ref('stg_nicu_chnd_flowsheets') }} as stg_nicu_chnd_flowsheets
        on cohort.log_key =  stg_nicu_chnd_flowsheets.log_key
    left join {{ ref('stg_nicu_chnd_procedures') }} as stg_nicu_chnd_procedures
        on cohort.log_key = stg_nicu_chnd_procedures.log_key
    left join {{ ref('stg_nicu_chnd_blood') }} as stg_nicu_chnd_blood
        on cohort.log_key = stg_nicu_chnd_blood.log_key
    left join {{ ref('stg_nicu_chnd_medications') }} as stg_nicu_chnd_medications
        on cohort.log_key = stg_nicu_chnd_medications.log_key
