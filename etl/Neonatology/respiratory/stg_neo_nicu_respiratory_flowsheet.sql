select distinct
    stg_neo_nicu_visit_demographics.visit_key,
    flowsheet_all.flowsheet_id,
    flowsheet_all.flowsheet_name,
    flowsheet_all.recorded_date,
    flowsheet_all.meas_val,
    flowsheet_all.meas_val_num
from
    {{ ref('stg_neo_nicu_visit_demographics') }} as stg_neo_nicu_visit_demographics
    inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on flowsheet_all.visit_key = stg_neo_nicu_visit_demographics.visit_key
where
    flowsheet_all.flowsheet_id in (
        7609, /* Respiratory Rate (Vent) */
        40000234, /* O2 Flow Rate (Lpm) */
        40000242, /* Resp/O2 Device */
        40000705, /* HFOV Frequency (Hz) */
        40002468, /* FiO2 (%) */
        40002606, /* HFJV PIP Set (cm H2O) */
        40002607, /* HFJV  Ti (sec) */
        40002714, /* IPAP Set (cmH2O) */
        40002715, /* IPAP Actual (cmH2O) */
        40002718, /* Mode, Non-Invasive */
        40002720, /* Interface, Non-Invasive */
        40002723, /* HFOV Alarm High */
        40002731, /* Ti, Non-Invasive (sec) */
        40008116, /* Artificial Airway */
        40010970, /* HFJV Change in Pressure */
        40010975, /* HFOV Power Set */
        40010977, /* HFOV Amplitude Actual */
        40060908, /* Flow Mode */
        40010880, /* (group = Endotracheal Tube) Secured Status */
        40010941, /* Invasive Device */
        40010942 /* (group = Invasive Ventilation) Mode */
    )
