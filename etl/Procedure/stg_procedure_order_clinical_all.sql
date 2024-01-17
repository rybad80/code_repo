{{ config(materialized='table', dist='visit_key') }}

select
    procedure_order.proc_ord_key,
    procedure.proc_nm as procedure_name,
    coalesce(procedure_order.cpt_cd, procedure.cpt_cd) as cpt_code,
    procedure_order.placed_dt as placed_date,
    stg_procedure_order_child_orders.parent_placed_dt as parent_placed_date,
    dict_ord_type.dict_nm as procedure_group_name,
    case
        when procedure_group.proc_grp_nm = 'NOT APPLICABLE' then procedure.proc_cat
        else procedure_group.proc_grp_nm
    end as procedure_subgroup_name,
    procedure_order.proc_ord_rec_type,
    protocol.ptcl_nm,
    stg_procedure_order_child_orders.orderset_name,
    dict_ord_stat.dict_nm as order_status,
    department.dept_nm as department_name,
    stg_procedure_order_child_orders.parent_department_name,
    dict_ord_class.dict_nm as order_class,
    dict_spec_src.dict_nm as order_specimen_source,
    procedure_order.specimen_taken_dt as specimen_taken_date,
    procedure_order.rslt_dt as result_date,
    procedure_order.abnorm_ind as abnormal_result_ind,
    stg_procedure_order_abnormal_results.abnormal_result_date,
    provider.full_nm,
    stg_procedure_order_child_orders.parent_ord_provider_name,
    procedure_order.proc_ord_id as procedure_order_id,
    procedure.proc_id as procedure_id,
    procedure_order.pat_key,
    procedure_order.visit_key,
    department.dept_key,
    department.dept_id as department_id,
    stg_procedure_order_child_orders.parent_dept_key,
    stg_procedure_order_child_orders.proc_ord_parent_key,
    provider.prov_key,
    stg_procedure_order_child_orders.parent_ord_prov_key
from
    {{source('cdw', 'procedure_order')}} as procedure_order
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = procedure_order.pat_key
    left join {{ref('stg_procedure_order_child_orders')}} as stg_procedure_order_child_orders
        on stg_procedure_order_child_orders.proc_ord_key = procedure_order.proc_ord_key
    inner join {{source('cdw', 'procedure')}} as procedure --noqa: L029
        on procedure.proc_key = procedure_order.proc_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_ord_stat
        on dict_ord_stat.dict_key = procedure_order.dict_ord_stat_key
    inner join {{source('cdw', 'department')}} as department
        on department.dept_key = procedure_order.pat_loc_dept_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_ord_class
        on dict_ord_class.dict_key = procedure_order.dict_ord_class_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_spec_src
        on dict_spec_src.dict_key = procedure_order.dict_spec_src_key
    inner join {{source('cdw', 'protocol')}} as protocol
        on protocol.ptcl_key = procedure_order.ptcl_key
    inner join {{source('cdw', 'procedure_group')}} as procedure_group
        on procedure_group.proc_grp_key = procedure.proc_grp_key
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = procedure_order.ordering_prov_key
    left join {{ref('stg_procedure_order_abnormal_results')}} as stg_procedure_order_abnormal_results
        on stg_procedure_order_abnormal_results.proc_ord_key = procedure_order.proc_ord_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_ord_type
        on dict_ord_type.dict_key = procedure_order.dict_ord_type_key
where
    procedure_order.create_by = 'CLARITY'
