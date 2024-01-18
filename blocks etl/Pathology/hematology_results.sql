with specimen as (
    (select
        spec_db_main.specimen_id,
        stg_patient.mrn,
        null as requisition_grouper_name,
        stg_patient.mrn as specimen_patient_id,
        stg_patient.dob,
        spec_db_main.spec_coll_dept_id as specimen_collected_department_id,
        spec_db_main.spec_dtm_collected as specimen_collected_datetime,
        spec_db_main.spec_number_ln1 as specimen_number_line_1
    from
        {{source('clarity_ods', 'spec_db_main')}} as spec_db_main
        inner join {{ref('stg_patient')}} as stg_patient
            on spec_db_main.spec_ept_pat_id = stg_patient.pat_id)
    union all
    (select
        spec_db_main.specimen_id,
        null as mrn,
        rqg_db_main.rqg_grouper_name as requisition_grouper_name,
        -- For Apheresis requisitions, pull patient ID from the requisition grouper name
        substr(
            rqg_db_main.rqg_grouper_name,
            instr(rqg_db_main.rqg_grouper_name, ',') + 1,
            length(rqg_db_main.rqg_grouper_name))
        as specimen_patient_id,
        null as dob,
        spec_db_main.spec_coll_dept_id as specimen_collected_department_id,
        spec_db_main.spec_dtm_collected as specimen_collected_datetime,
        spec_db_main.spec_number_ln1 as specimen_number_line_1
    from
        {{source('clarity_ods', 'spec_db_main')}} as spec_db_main
        inner join {{source('clarity_ods', 'rqg_db_main')}} as rqg_db_main
            on rqg_db_main.rqg_grouper_id = spec_db_main.spec_req_grp_id
    where lower(rqg_db_main.rqg_grouper_name) like 'zaph%') -- Include Apheresis requisition specimens 
)
select
    res_db_main.result_id,
    res_components.line,
    specimen.specimen_id,
    test_mstr_db_main.test_id,
    clarity_component.component_id,
    specimen.mrn,
    specimen.requisition_grouper_name,
    specimen.specimen_patient_id,
    specimen.dob,
    specimen.specimen_collected_department_id,
    clarity_dep.department_name,
    specimen.specimen_collected_datetime,
    specimen.specimen_number_line_1,
    order_proc.order_proc_id as procedure_order_id,
    test_mstr_db_main.test_name,
    test_mstr_db_main.test_abbr as test_abbreviation,
    clarity_component.name as component_name,
    res_components.component_result,
    case
        when res_components.component_result is null then 0
        else 1
    end as component_result_present_ind,
    case
        when lower(lab_section.section_name) like '%ua and bf%' then 1
        else 0
    end as ua_and_bf_ind, -- Indicator for Uranalysis and Body Fluid sections
    case
        when spec_test_rel.spec_tst_id in (
            123050014,
            123050016,
            123050020,
            123050006,
            123050002,
            123050021,
            123050007,
            123051700,
            123050017,
            123050022
        ) then 0
        else 1
    end as filemaker_import_test_ind -- Indicator to exclude tests irrelevant to Apheresis Filemaker system
from specimen
inner join {{source('clarity_ods', 'spec_test_rel')}} as spec_test_rel
    on specimen.specimen_id = spec_test_rel.specimen_id
inner join {{source('clarity_ods', 'test_mstr_db_main')}} as test_mstr_db_main
    on spec_test_rel.spec_tst_id = test_mstr_db_main.test_id
inner join {{source('clarity_ods', 'res_db_main')}} as res_db_main
    on spec_test_rel.specimen_id = res_db_main.res_specimen_id
        and spec_test_rel.spec_tst_id = res_db_main.res_test_id
inner join {{source('clarity_ods', 'res_components')}} as res_components
    on res_db_main.result_id = res_components.result_id
inner join  {{source('clarity_ods', 'order_proc')}} as order_proc
    on order_proc.order_proc_id = spec_test_rel.spec_tst_order_id
inner join {{source('clarity_ods', 'zc_res_val_status')}} as zc_res_val_status
    on zc_res_val_status.res_val_status_c = res_components.comp_verif_status_c
inner join {{source('clarity_ods', 'clarity_component')}} as clarity_component
    on res_components.component_id = clarity_component.component_id
left join {{source('clarity_ods', 'clarity_dep')}} as clarity_dep
    on specimen.specimen_collected_department_id = clarity_dep.department_id
inner join {{source('clarity_ods', 'lab_section')}} as lab_section
    on spec_test_rel.spec_tst_sec_id = lab_section.section_id
inner join {{ref('lookup_lab_section_department_grouper')}} as lookup_lab_section_department_grouper
    on lookup_lab_section_department_grouper.section_id = lab_section.section_id
where
    lower(lookup_lab_section_department_grouper.section_department_grouper) = 'hematology'
    and lower(zc_res_val_status.name) = 'verified'
    -- Exclude metaresult records that contain no clinical result data
    and lower(clarity_component.name) != 'results complete'
