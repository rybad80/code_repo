select
        fs_rec_key,
        seq_num,
        cardiac_perfusion_surgery.visit_key,
        cardiac_perfusion_surgery.anes_visit_key,
        cardiac_perfusion_surgery.log_key,
        cardiac_perfusion_surgery.patient_name,
        cardiac_perfusion_surgery.mrn,
        cardiac_perfusion_surgery.dob,
        encounter_date,
        cardiac_perfusion_surgery.csn,
        hospital_admit_date,
        flowsheet_name,
        flowsheet_id,
        flowsheet_record_id,
        meas_val,
        meas_val_num,
        meas_cmt,
        recorded_date,
        entry_date,
        documented_by_employee,
        taken_by_employee,
        documented_by_employee_key,
        taken_by_employee_key,
        vsi_key,
        fs_key,
        cardiac_perfusion_surgery.pat_key,
        block_last_update_date
   from
    {{ref('flowsheet_all')}} as flowsheet_all
    inner join {{ref('cardiac_perfusion_surgery')}} as cardiac_perfusion_surgery
        on cardiac_perfusion_surgery.anes_visit_key = flowsheet_all.anes_visit_key
