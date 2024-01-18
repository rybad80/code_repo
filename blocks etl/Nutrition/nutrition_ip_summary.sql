select
    encounter_inpatient.visit_key,
    encounter_inpatient.pat_key,
    encounter_inpatient.mrn,
    encounter_inpatient.dob,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.hospital_discharge_date,
    {% for _, col_name in get_nutrition_ip_flowsheets().items() %}
        initial_flowsheets.{{ col_name }} as {{ col_name }}_initial,
        final_flowsheets.{{ col_name }} as {{ col_name }}_final,
    {% endfor %}
    initial_feeding_type.feeding_type as feeding_type_initial,
    final_feeding_type.feeding_type as feeding_type_final,
    po_intake_initial.intake_po_percent as intake_po_percent_initial,
    po_intake_final.intake_po_percent as intake_po_percent_final

from
    {{ ref('encounter_inpatient') }} as encounter_inpatient
    left join {{ ref('stg_nutrition_ip_flowsheet_pivot') }} as initial_flowsheets
        on initial_flowsheets.visit_key = encounter_inpatient.visit_key
        and initial_flowsheets.timing = 'initial'
    left join {{ ref('stg_nutrition_ip_flowsheet_pivot') }} as final_flowsheets
        on final_flowsheets.visit_key = encounter_inpatient.visit_key
        and final_flowsheets.timing = 'final'
    left join {{ ref('stg_nutrition_ip_feeding_type') }} as initial_feeding_type
        on  initial_feeding_type.visit_key = encounter_inpatient.visit_key
        and initial_feeding_type.feeding_rn_visit_asc = 1
    left join {{ ref('stg_nutrition_ip_feeding_type') }} as final_feeding_type
        on  final_feeding_type.visit_key = encounter_inpatient.visit_key
        and final_feeding_type.feeding_rn_visit_desc = 1
    left join {{ ref('stg_nutrition_ip_po_intake') }} as po_intake_initial
        on  po_intake_initial.visit_key = encounter_inpatient.visit_key
        and po_intake_initial.index_date = date(encounter_inpatient.hospital_admit_date)
    left join {{ ref('stg_nutrition_ip_po_intake') }} as po_intake_final
        on  po_intake_final.visit_key = encounter_inpatient.visit_key
        and po_intake_final.index_date = coalesce(
            encounter_inpatient.hospital_discharge_date,
            current_date - 1
        )
