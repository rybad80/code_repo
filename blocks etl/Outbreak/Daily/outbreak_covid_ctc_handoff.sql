{{ config(meta = {
    'critical': true
}) }}

select
    record_id,
    emp_fname,
    emp_lname,
    pat_fname,
    pat_lname,
    email_address_follow_up,
    test_date,
    today_date,
    coalesce(test_date, today_date) as record_date,
    patient_mrn,
    patient_or_employee,
    upd_dt,
    outpatient_setting,
    inpatient_unit,
    inpatient_bed_space,
    additional_info,
    patient_date_positive,
    patient_exposures,
    employee_patient_exposures,
    employee_exposures,
    employee_exposures_qty,
    cast(date_trunc('week', (record_date + interval '1 day')) - interval '1 day' as date) as current_week,
    trim(exposure_at_work) as exposure_at_work,
    ppe_donned_during_exposure,
    if_yes_which_ppe___1 as ppe_donned_gloves,
    if_yes_which_ppe___2 as ppe_donned_mask,
    if_yes_which_ppe___3 as ppe_donned_gown,
    if_yes_which_ppe___4 as ppe_donned_goggles,
    if_yes_which_ppe___5 as ppe_donned_respirator,
    if_yes_which_ppe___99 as ppe_donned_other,
    positive_exp_loc,
    agp_exposure,
    ppe_breach,
    ppe_breach_select___1 as ppe_breach_gloves,
    ppe_breach_select___2 as ppe_breach_mask,
    ppe_breach_select___3 as ppe_breach_gown,
    ppe_breach_select___4 as ppe_breach_goggles,
    ppe_breach_select___5 as ppe_breach_respirator,
    ppe_breach_select___99 as ppe_breach_other,
    loc_exposure_bldg,
    trim(exposure_source_at_work) as exposure_source_at_work,
    risk_level

from {{source('ods', 'ctc_handoff')}}

where
    redcap_repeat_instrument is null
    