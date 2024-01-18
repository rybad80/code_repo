select
    encounter_inpatient.visit_key,
    date(flowsheet_all.recorded_date) as index_date,
    sum(case when flowsheet_all.flowsheet_id = 40000239 then flowsheet_all.meas_val_num else 0 end) as po_intake,
    sum(flowsheet_all.meas_val_num) as all_intake,
    case
        when all_intake = 0 then null
        else round(po_intake / all_intake, 2)
    end as intake_po_percent

from
    {{ ref('encounter_inpatient') }} as encounter_inpatient
    inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on flowsheet_all.visit_key = encounter_inpatient.visit_key
    inner join {{ ref('lookup_nutrition_ip_intake_flowsheets' )}} as lookup_nutrition_ip_intake_flowsheets
        on lookup_nutrition_ip_intake_flowsheets.flowsheet_id = flowsheet_all.flowsheet_id

where
    flowsheet_all.meas_val_num is not null

group by
    encounter_inpatient.visit_key,
    date(flowsheet_all.recorded_date)
