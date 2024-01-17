select
    {{dbt_utils.surrogate_key([
                'stg_procedure_order_encounter.proc_ord_key',
                'procedure_order_result.seq_num'
            ])
    }} as procedure_order_result_key,
    stg_procedure_order_encounter.proc_ord_key,
    procedure_order_result.seq_num as result_seq_num,
    1 as component_seq_num,
    stg_procedure_order_encounter.patient_name,
    stg_procedure_order_encounter.mrn,
    stg_procedure_order_encounter.dob,
    stg_procedure_order_encounter.csn,
    stg_procedure_order_encounter.encounter_date,
    stg_procedure_order_encounter.procedure_name,
    stg_procedure_order_encounter.cpt_code,
    result_component.comp_nm as result_component_name,
    result_component.ext_nm as result_component_external_name,
    result_component.rslt_comp_id as result_component_id,
    stg_procedure_order_encounter.placed_date,
    stg_procedure_order_encounter.order_specimen_source,
    stg_procedure_order_encounter.specimen_taken_date,
    stg_procedure_order_encounter.parent_placed_date,
    stg_procedure_order_encounter.procedure_group_name,
    stg_procedure_order_encounter.procedure_subgroup_name,
    procedure_order_result.rslt_val as result_value,
    case
        when procedure_order_result.rslt_num_val = 9999999 then null
        else procedure_order_result.rslt_num_val
    end as result_value_numeric,
    procedure_order_result.ref_unit as reference_unit,
    procedure_order_result.ref_low as reference_low_value,
    procedure_order_result.ref_high as reference_high_value,
    dict_rslt_stat.dict_nm as result_status,
    dict_lab_stat.dict_nm as result_lab_status,
    case 
        when lower(procedure_order_result.rslt_val) = 'tnp'
            or (
                lower(dict_lab_stat.dict_nm) in ('final result', 'edited', 'edited result - final')
                and procedure_order_result.rslt_val is null
                and stg_procedure_order_encounter.specimen_taken_date > '05-21-2021'
                )
            then 1 
        when procedure_order_result.rslt_val is null
            and stg_procedure_order_encounter.specimen_taken_date < '05-21-2021' 
            then null 
        else 0 
    end as test_cancelled_ind, 
    stg_procedure_order_encounter.ptcl_nm as orderset_name,
    stg_procedure_order_encounter.department_name,
    stg_procedure_order_encounter.parent_department_name,
    procedure_order_result.rslt_dt as result_date,
    case
        when lower(dict_abnormal_ind.dict_nm) = 'not applicable' then 0
        else 1
    end as abnormal_result_ind,
    stg_procedure_order_encounter.full_nm as ordering_provider_name,
    stg_procedure_order_encounter.parent_ord_provider_name,
    stg_procedure_order_encounter.procedure_order_id,
    stg_procedure_order_encounter.procedure_id,
    stg_procedure_order_encounter.pat_key,
    stg_procedure_order_encounter.visit_key,
    stg_procedure_order_encounter.dept_key,
    stg_procedure_order_encounter.department_key,
    result_component.rslt_comp_key,
    stg_procedure_order_encounter.parent_dept_key,
    stg_procedure_order_encounter.proc_ord_parent_key,
    stg_procedure_order_encounter.prov_key as ordering_provider_key,
    stg_procedure_order_encounter.parent_ord_prov_key,
    procedure_order_result.upd_dt as update_date
from
    {{ref('stg_procedure_order_encounter')}} as stg_procedure_order_encounter
    inner join {{source('cdw', 'procedure_order_result')}} as procedure_order_result
        on procedure_order_result.proc_ord_key = stg_procedure_order_encounter.proc_ord_key
    inner join {{source('cdw', 'result_component')}} as result_component
        on result_component.rslt_comp_key = procedure_order_result.rslt_comp_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_rslt_stat
        on dict_rslt_stat.dict_key = procedure_order_result.dict_rslt_stat_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_lab_stat
        on dict_lab_stat.dict_key = procedure_order_result.dict_lab_stat_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_abnormal_ind
        on dict_abnormal_ind.dict_key = procedure_order_result.dict_abnorm_cd_key
