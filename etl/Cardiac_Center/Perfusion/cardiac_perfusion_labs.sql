select
    procedure_order_result_clinical.proc_ord_key,
    result_seq_num,
    component_seq_num,
    cardiac_perfusion_surgery.patient_name,
    cardiac_perfusion_surgery.mrn,
    cardiac_perfusion_surgery.dob,
    cardiac_perfusion_surgery.csn,
    encounter_date,
    procedure_name,
    cpt_code,
    result_component_name,
    result_component_external_name,
    result_component_id,
    placed_date,
    order_specimen_source,
	lab_type.dict_nm as lab_type,
    specimen_taken_date,
    parent_placed_date,
    procedure_group_name,
    procedure_subgroup_name,
    result_value,
    result_value_numeric,
    reference_unit,
    reference_low_value,
    reference_high_value,
    result_status,
    result_lab_status,
    orderset_name,
    department_name,
    parent_department_name,
    result_date,
    abnormal_result_ind,
    ordering_provider_name,
    parent_ord_provider_name,
    procedure_order_id,
    procedure_id,
    cardiac_perfusion_surgery.pat_key,
    procedure_order_result_clinical.visit_key,
    dept_key,
    rslt_comp_key,
    parent_dept_key,
    procedure_order_result_clinical.proc_ord_parent_key,
    ordering_provider_key,
    parent_ord_prov_key
from
     {{ref('cardiac_perfusion_surgery')}} as cardiac_perfusion_surgery
     inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
     on  procedure_order_result_clinical.visit_key = cardiac_perfusion_surgery.visit_key
     inner join {{source('cdw', 'procedure_order')}} as procedure_order
     on procedure_order_result_clinical.proc_ord_key = procedure_order.proc_ord_key
     inner join {{source('cdw', 'cdw_dictionary')}} as lab_type
     on lab_type.dict_key = procedure_order.dict_spec_type_key
