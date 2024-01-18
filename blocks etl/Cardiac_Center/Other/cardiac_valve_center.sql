select
    record_id,
    chop_mrn_if_applicable as mrn,
    patient_first_name_last_na as patient_name,
    date_of_birth as dob,
    date_of_referral,
    referring_provider,
    location_hospital_of_cardi as cardiologist_location,
    referring_physician_contac as referring_physician_contact,
    case
        when recent_cardiology_testing___1 = 'checked'
            then 1
        else 0
        end as echo_last_6_months,
    case
        when recent_cardiology_testing___2 = 'checked'
            then 1
        else 0
        end as tee_tte_last_6_months,
    case
        when recent_cardiology_testing___3 = 'checked'
            then 1
        else 0
        end as mri_last_6_months,
    case
        when recent_cardiology_testing___4 = 'checked'
            then 1
        else 0
        end as cardiac_cath_last_6_months,
    case
        when recent_cardiology_testing___5 = 'checked'
            then 1
        else 0
        end as exercise_testing_last_6_months,
    is_a_cardiac_mri_3d_echo_o as advanced_imaging_in_next_month,
    primary_cardiac_diagnosis,
    reason_for_referral,
    urent_referral_average_tur as urgent_referral,
    please_provide_us_with_any as comments
from
    {{source('ods_redcap_research', 'cardiac_valve_center')}}
