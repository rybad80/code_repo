/* stg_nutrition_ip_feeding_type
This code looks for the presence of particular flowsheets that tell us if a patient is
receiving enteral or parenteral nutrition. */

select
    encounter_inpatient.visit_key,
    date(flowsheet_all.recorded_date) as index_date,
    master_date.c_wk_start_dt,
    master_date.c_wk_end_dt,
    /* changed 'EN%' / 'PN%' to '%EN%' / '%PN%' -- to capture these:
    Weight used in PN Order (kg)
    Read Only EN Protein (g/kg)
    Read Only EN kcal/kg
    */
    max(case when flowsheet_all.flowsheet_name like '%EN%' then 1 else 0 end) as enteral_ind,
    max(case when flowsheet_all.flowsheet_name like '%PN%' then 1 else 0 end) as parenteral_ind,
    case
        when enteral_ind = 1 and parenteral_ind = 1 then 'Enteral and Parenteral'
        when enteral_ind = 1 then 'Enteral'
        when parenteral_ind = 1 then 'Parenteral'
    end as feeding_type,
    /* These row number variables are used to determine:
        a. the first feeding type recorded for the hospitalization (feeding_rn_visit_asc)
        b. the final feeding type recorded for the hospitalization (feeding_rn_visit_desc)
        c. the final feeding type recorded for each week (feeding_rn_week_desc)
        We'll need these variables when joining to our weekly and overall summary tables. */
    row_number() over (
        partition by
            encounter_inpatient.visit_key
        order by
            index_date asc
    ) as feeding_rn_visit_asc,
    row_number() over (
        partition by
            encounter_inpatient.visit_key
        order by
            index_date desc
    ) as feeding_rn_visit_desc,
    row_number() over (
        partition by
            encounter_inpatient.visit_key,
            master_date.c_wk_start_dt
        order by
            index_date desc
    ) as feeding_rn_week_desc

from
    {{ ref('encounter_inpatient') }} as encounter_inpatient
    inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on flowsheet_all.visit_key = encounter_inpatient.visit_key
        and date(flowsheet_all.recorded_date) between date(encounter_inpatient.hospital_admit_date)
            and coalesce(date(encounter_inpatient.hospital_discharge_date), current_date)
    inner join {{ ref('flowsheet_group_lookup') }} as flowsheet_group_lookup
        on flowsheet_group_lookup.flowsheet_id = flowsheet_all.flowsheet_id
        and flowsheet_group_lookup.template_id = 40062050 /* IP Clinical Nutrition Assessment */
        and flowsheet_group_lookup.group_id in (
            40062018, /* Enteral Nutrition Detail */
            40062010 /* PN Order Detail */
        )
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.c_wk_start_dt <= flowsheet_all.recorded_date
        and master_date.c_wk_end_dt >= flowsheet_all.recorded_date
        /* limit master_date to just one row per week to avoid repeating rows */
        and master_date.day_of_wk = 1

where
    flowsheet_all.meas_val is not null

group by
    encounter_inpatient.visit_key,
    date(flowsheet_all.recorded_date),
    master_date.c_wk_start_dt,
    master_date.c_wk_end_dt
