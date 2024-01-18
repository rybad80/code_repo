select
    encounter_inpatient.visit_key,
    master_date.c_wk_start_dt,
    master_date.c_wk_end_dt,
    encounter_inpatient.pat_key,
    encounter_inpatient.mrn,
    encounter_inpatient.dob,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.hospital_discharge_date,
    {% for _, col_name in get_nutrition_ip_flowsheets().items() %}
        stg_nutrition_ip_flowsheet_pivot.{{ col_name }},
    {% endfor %}
    stg_nutrition_ip_feeding_type.feeding_type,
    stg_nutrition_ip_po_intake.intake_po_percent,
    stg_nutrition_ip_malnutrition_dx.malnutrition_dx_ind

from
    {{ ref('encounter_inpatient') }} as encounter_inpatient
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.c_wk_start_dt <= coalesce(encounter_inpatient.hospital_discharge_date, current_date)
        and master_date.c_wk_end_dt >= encounter_inpatient.hospital_admit_date
        /* limit master_date to one row per week */
        and master_date.day_of_wk = 1
        /* only include completed weeks */
        and master_date.c_wk_end_dt < current_date
    left join {{ ref('stg_nutrition_ip_flowsheet_pivot') }} as stg_nutrition_ip_flowsheet_pivot
        on stg_nutrition_ip_flowsheet_pivot.visit_key = encounter_inpatient.visit_key
        and stg_nutrition_ip_flowsheet_pivot.c_wk_start_dt = master_date.c_wk_start_dt
    left join {{ ref('stg_nutrition_ip_feeding_type') }} as stg_nutrition_ip_feeding_type
        on  stg_nutrition_ip_feeding_type.visit_key = encounter_inpatient.visit_key
        and stg_nutrition_ip_feeding_type.c_wk_start_dt = master_date.c_wk_start_dt
        and stg_nutrition_ip_feeding_type.feeding_rn_week_desc = 1
    left join {{ ref('stg_nutrition_ip_po_intake') }} as stg_nutrition_ip_po_intake
        on stg_nutrition_ip_po_intake.visit_key = encounter_inpatient.visit_key
        and stg_nutrition_ip_po_intake.index_date = master_date.c_wk_end_dt
    left join {{ ref('stg_nutrition_ip_malnutrition_dx') }} as stg_nutrition_ip_malnutrition_dx
        on  stg_nutrition_ip_malnutrition_dx.visit_key = encounter_inpatient.visit_key
        and stg_nutrition_ip_malnutrition_dx.c_wk_start_dt = master_date.c_wk_start_dt
        and stg_nutrition_ip_malnutrition_dx.malnutrition_dx_week_desc = 1
