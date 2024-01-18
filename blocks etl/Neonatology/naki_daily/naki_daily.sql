{% set aminoglycosides = ['amikacin', 'gentamicin', 'tobramycin', 'vancomycin'] %}

select
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_naki_daily.index_date as naki_list_date,
    stg_naki_daily.episode_start_date as nicu_admit_date,
    stg_naki_daily.episode_end_date as nicu_discharge_date,
    stg_patient.dob,
    stg_patient.gestational_age_complete_weeks,
    stg_patient.gestational_age_remainder_days,
    stg_naki_daily.index_date - date(stg_patient.dob) as actual_age_in_days,
    case
        when stg_patient.gestational_age_complete_weeks < 37
        then actual_age_in_days + stg_patient.gestational_age_complete_weeks * 7
            + stg_patient.gestational_age_remainder_days - 40 * 7
    end as corrected_age_in_days,
    stg_naki_daily.ntmx_meds_daily_count,
    stg_naki_daily.ntmx_meds_daily_names,
    stg_naki_daily.any_ntmx_med_start_date,
    stg_naki_daily.any_ntmx_med_end_date,
    stg_naki_daily.any_ntmx_med_dot,
    {% for med in aminoglycosides %}
        stg_naki_daily.{{ med }}_start,
        stg_naki_daily.{{ med }}_end,
        stg_naki_daily.{{ med }}_duration_in_hours,
    {% endfor %}
    stg_naki_scr.result_value as latest_creatinine,
    stg_naki_scr.specimen_taken_date as latest_creatinine_collect_time
from
    {{ ref('stg_naki_daily') }} as stg_naki_daily
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = stg_naki_daily.pat_key
    left join {{ ref('stg_naki_scr') }} as stg_naki_scr
        on  stg_naki_scr.visit_key = stg_naki_daily.visit_key
        and stg_naki_scr.index_date = stg_naki_daily.index_date
        and stg_naki_scr.rn = 1
