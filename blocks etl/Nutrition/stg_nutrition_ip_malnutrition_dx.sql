select
    encounter_inpatient.visit_key,
    master_date.c_wk_start_dt,
    master_date.c_wk_end_dt,
    flowsheet_all.recorded_date,
    case
        when flowsheet_all.meas_val = 'Mild malnutrition' then 1
        when flowsheet_all.meas_val = 'Moderate malnutrition' then 1
        when flowsheet_all.meas_val = 'Severe malnutrition' then 1
        when flowsheet_all.meas_val = 'No malnutrition' then 0
        when flowsheet_all.meas_val = 'No current malnutrition, history of malnutrition within the last 6 months'
        then 0
    end as malnutrition_dx_ind,
    row_number() over (
        partition by
            encounter_inpatient.visit_key,
            master_date.c_wk_start_dt
        order by
            /* order by descending recorded_date so that we can easily find the most recent dx per week */
            flowsheet_all.recorded_date desc
    ) as malnutrition_dx_week_desc

from
    {{ ref('encounter_inpatient') }} as encounter_inpatient
    inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on encounter_inpatient.visit_key = flowsheet_all.visit_key
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.c_wk_start_dt <= flowsheet_all.recorded_date
        and master_date.c_wk_end_dt >= flowsheet_all.recorded_date
        /* limit master_date to just one row per week */
        and master_date.day_of_wk = 1

where
    flowsheet_all.flowsheet_id in (
        /* both named `diagnosis` */
        13771,
        17608
    )
    and malnutrition_dx_ind is not null
