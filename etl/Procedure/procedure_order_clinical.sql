{{
    config(materialized = 'view')
}}
select
    stg_procedure_order_encounter.proc_ord_key,
    stg_procedure_order_encounter.patient_name,
    stg_procedure_order_encounter.mrn,
    stg_procedure_order_encounter.pat_id,
    stg_procedure_order_encounter.dob,
    stg_procedure_order_encounter.csn,
    stg_procedure_order_encounter.encounter_date,
    stg_procedure_order_encounter.procedure_name,
    stg_procedure_order_encounter.cpt_code,
    stg_procedure_order_encounter.placed_date,
    stg_procedure_order_encounter.parent_placed_date,
    stg_procedure_order_encounter.procedure_group_name,
    stg_procedure_order_encounter.procedure_subgroup_name,
    case
        when stg_procedure_order_encounter.proc_ord_rec_type = 'P' then 'Parent Order'
        when stg_procedure_order_encounter.proc_ord_rec_type = 'F' then 'Future Order'
        when stg_procedure_order_encounter.proc_ord_rec_type = 'N' then 'Normal (Single) Order'
        when stg_procedure_order_encounter.proc_ord_rec_type = 'C' then 'Child Order'
        else null
    end as procedure_order_type,
    case
        when
            stg_procedure_order_encounter.proc_ord_rec_type = 'C'
                then stg_procedure_order_encounter.orderset_name
        else stg_procedure_order_encounter.ptcl_nm
    end as orderset_name,
    stg_procedure_order_encounter.order_status,
    stg_procedure_order_encounter.department_name,
    coalesce(stg_procedure_order_encounter.parent_department_name, 'UNKNOWN') as parent_department_name,
    stg_procedure_order_encounter.order_class,
    stg_procedure_order_encounter.order_specimen_source,
    stg_procedure_order_encounter.specimen_taken_date,
    stg_procedure_order_encounter.result_date,
    stg_procedure_order_encounter.abnormal_result_ind,
    stg_procedure_order_encounter.abnormal_result_date,
    coalesce(
        stg_procedure_order_encounter.full_nm, stg_procedure_order_encounter.parent_ord_provider_name
    ) as ordering_provider_name,
    stg_procedure_order_encounter.procedure_order_id,
    stg_procedure_order_encounter.procedure_id,
    stg_procedure_order_encounter.pat_key,
    stg_procedure_order_encounter.visit_key,
    stg_procedure_order_encounter.encounter_key,
    stg_procedure_order_encounter.dept_key,
    stg_procedure_order_encounter.department_id,
    coalesce(stg_procedure_order_encounter.parent_dept_key, 0) as parent_dept_key,
    coalesce(stg_procedure_order_encounter.proc_ord_parent_key, 0) as proc_ord_parent_key,
    coalesce(
        case
            when
                stg_procedure_order_encounter.proc_ord_rec_type = 'C'
                    then stg_procedure_order_encounter.parent_ord_prov_key
            else stg_procedure_order_encounter.prov_key
        end,
        0
    ) as ordering_provider_key
from
    {{ref('stg_procedure_order_encounter')}} as stg_procedure_order_encounter
