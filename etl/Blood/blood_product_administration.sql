select
     procedure_order_clinical.proc_ord_key,
     procedure_order_clinical.patient_name,
     procedure_order_clinical.mrn,
     procedure_order_clinical.pat_id,
     procedure_order_clinical.dob,
     procedure_order_clinical.csn,
     procedure_order_clinical.encounter_date,
     procedure_order_clinical.procedure_name,
     procedure_order_clinical.placed_date,
     procedure_order_clinical.orderset_name,
     procedure_order_clinical.order_status,
     procedure_order_clinical.ordering_provider_name,
     procedure_order_clinical.department_name,
     procedure_order_clinical.procedure_order_id,
     procedure_order_clinical.procedure_id,
     order_proc.description as order_description,
     ord_blood_admin.blood_start_instant as blood_admin_start_date,
     ord_blood_admin.blood_end_instant as blood_admin_end_date,
     flowsheet_all.meas_val_num as blood_volume,
     replace(order_proc.display_name, 'Transfusion Order: ', '') as blood_product_type,
     ord_blood_admin.blood_product_code,
     ord_blood_admin.blood_unit_num as blood_unit_number,
     order_proc.display_name as order_display_name,
     flowsheet_all.flowsheet_name,
     flowsheet_all.flowsheet_id,
     flowsheet_all.occurance,
     flowsheet_all.meas_cmt as flowsheet_comment,
     case when lower(order_proc.description) like '%in prime%' then 1 else 0 end as prime_ind,
     flowsheet_all.recorded_date,
     flowsheet_all.entry_date,
     flowsheet_all.documented_by_employee,
     flowsheet_all.taken_by_employee,
     visit_stay_info.vsi_key,
     procedure_order_clinical.pat_key,
     procedure_order_clinical.visit_key,
     anesthesia_encounter_link.or_log_key
 from
    {{ref('procedure_order_clinical')}} as procedure_order_clinical
    inner join {{source('cdw', 'visit_stay_info')}} as  visit_stay_info
        on visit_stay_info.visit_key = procedure_order_clinical.visit_key
    inner join {{source('cdw', 'visit_stay_info_rows')}} as visit_stay_info_rows
        on visit_stay_info_rows.vsi_key = visit_stay_info.vsi_key
    inner join {{source('cdw', 'visit_stay_info_rows_order')}} as visit_stay_info_rows_order
        on visit_stay_info_rows_order.vsi_key = visit_stay_info_rows.vsi_key
           and visit_stay_info_rows.seq_num = visit_stay_info_rows_order.seq_num
           and visit_stay_info_rows_order.ord_key = procedure_order_clinical.proc_ord_key
    inner join {{source('clarity_ods', 'ord_blood_admin')}} as ord_blood_admin
        on procedure_order_clinical.procedure_order_id = ord_blood_admin.order_id
    inner join {{source('clarity_ods', 'order_proc')}} as order_proc
        on ord_blood_admin.order_id = order_proc.order_proc_id
    inner join {{ref('flowsheet_all')}} as flowsheet_all
        on flowsheet_all.vsi_key = visit_stay_info_rows.vsi_key
           and visit_stay_info_rows.seq_num = flowsheet_all.occurance
    left join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
        on anesthesia_encounter_link.anes_visit_key = procedure_order_clinical.visit_key
where
    procedure_order_type = 'Child Order'
    and flowsheet_id = 500025331
