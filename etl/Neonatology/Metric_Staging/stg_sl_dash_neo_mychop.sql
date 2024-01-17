select
    stg_sl_dash_neo_visits_discharged.visit_key,
    stg_sl_dash_neo_visits_discharged.hospital_discharge_date,
    max(
        case
            /* activated */
            when pat_myc_stat_hx.myc_stat_hx_c = 1
                and pat_myc_stat_hx.myc_stat_hx_tmstp < stg_sl_dash_neo_visits_discharged.hospital_admit_date
                then 1
            else 0
        end
    ) as mychop_activated_before_admission_ind,
    max(
        case
            /* declined */
            when pat_myc_stat_hx.myc_stat_hx_c = 5
                and pat_myc_stat_hx.myc_stat_hx_tmstp < stg_sl_dash_neo_visits_discharged.hospital_admit_date
                then 1
            else 0
        end
    ) as mychop_declined_before_admission_ind,
    max(
        case
            /* activated */
            when pat_myc_stat_hx.myc_stat_hx_c = 1
                and pat_myc_stat_hx.myc_stat_hx_tmstp between
                stg_sl_dash_neo_visits_discharged.hospital_admit_date
                and stg_sl_dash_neo_visits_discharged.hospital_discharge_date
                then 1
            else 0
        end
    ) as mychop_activated_during_admission_ind
from
    {{ ref('stg_sl_dash_neo_visits_discharged') }} as stg_sl_dash_neo_visits_discharged
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = stg_sl_dash_neo_visits_discharged.pat_key
    left join {{ source('clarity_ods', 'pat_myc_stat_hx') }} as pat_myc_stat_hx
        on pat_myc_stat_hx.pat_id = stg_patient.pat_id
group by
    stg_sl_dash_neo_visits_discharged.visit_key,
    stg_sl_dash_neo_visits_discharged.hospital_discharge_date
having
    mychop_activated_before_admission_ind + mychop_declined_before_admission_ind = 0
