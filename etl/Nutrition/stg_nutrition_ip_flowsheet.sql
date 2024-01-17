/* stg_nutrition_ip_flowsheet
This code pulls the key flowsheet data and performs some clean-up before being used
downstream in `stg_nutrition_ip_flowsheet_pivot`. */

select
    encounter_inpatient.visit_key,
    master_date.c_wk_start_dt,
    master_date.c_wk_end_dt,
    flowsheet_all.flowsheet_id,
    flowsheet_all.flowsheet_name,
    flowsheet_all.recorded_date,
    flowsheet_all.meas_val,
    flowsheet_all.meas_val_num,
    case
        /* convert ounces to grams for `weight` */
        when flowsheet_all.flowsheet_id = 14 then round(flowsheet_all.meas_val_num * 2.2 * 16)
        /* convert messy `+1.06` or `-1.33 SD` to numerics for weight and w4l z scores */
        when flowsheet_all.flowsheet_id in (12756, 13706) and flowsheet_all.meas_val_num is null
        then cast(regexp_extract(flowsheet_all.meas_val, '-?[0-9]\.[0-9]{1,2}') as numeric)
        else flowsheet_all.meas_val_num
    end as clean_meas_val_num,
    /* flowsheet_rn_visit_asc = 1 is our first row per flowsheet per hospitalization */
    row_number() over (
        partition by
            encounter_inpatient.visit_key,
            flowsheet_all.flowsheet_id
        order by
            flowsheet_all.recorded_date asc
    ) as flowsheet_rn_visit_asc,
    /* flowsheet_rn_visit_desc = 1 is our final row per flowsheet per hospitalization */
    row_number() over (
        partition by
            encounter_inpatient.visit_key,
            flowsheet_all.flowsheet_id
        order by
            flowsheet_all.recorded_date desc
    ) as flowsheet_rn_visit_desc,
    /* flowsheet_rn_week_desc = 1 is our final row per flowsheet per week */
    row_number() over (
        partition by
            encounter_inpatient.visit_key,
            master_date.c_wk_start_dt,
            flowsheet_all.flowsheet_id
        order by
            flowsheet_all.recorded_date desc
    ) as flowsheet_rn_week_desc

from 
    {{ ref('encounter_inpatient') }} as encounter_inpatient
    inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on flowsheet_all.visit_key = encounter_inpatient.visit_key
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.c_wk_start_dt <= flowsheet_all.recorded_date
        and master_date.c_wk_end_dt >= flowsheet_all.recorded_date
        /* limit master_date to just one row per week */
        and master_date.day_of_wk = 1

where 
    flowsheet_all.flowsheet_id in (
        {% for flowsheet_id in get_nutrition_ip_flowsheets().keys() %}
            {{ flowsheet_id }}{{ ',' if not loop.last }}
        {% endfor %}
    )
    and (
        clean_meas_val_num is not null
        or (
            /* z_score_w4l_malnutrition is always non-numeric */
            flowsheet_all.flowsheet_id = 400730782
            and flowsheet_all.meas_val is not null
        )
    )
