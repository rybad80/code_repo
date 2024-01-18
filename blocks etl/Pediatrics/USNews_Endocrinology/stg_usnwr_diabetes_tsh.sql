with proc_order as (
    select
        stg_usnwr_diabetes_primary_pop.primary_key,
        procedure_order_result_clinical.visit_key,
        procedure_order_result_clinical.procedure_name,
        procedure_order_result_clinical.result_component_name,
        procedure_order_result_clinical.procedure_group_name,
        procedure_order_result_clinical.abnormal_result_ind,
        procedure_order_result_clinical.result_value,
        procedure_order_result_clinical.result_value_numeric,
        procedure_order_result_clinical.result_date
    from
        {{ ref('stg_usnwr_diabetes_primary_pop') }} as stg_usnwr_diabetes_primary_pop
        inner join {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
            on procedure_order_result_clinical.pat_key = stg_usnwr_diabetes_primary_pop.pat_key
    where
        lower(procedure_order_result_clinical.result_lab_status) = 'final result' --only include final lab results
    group by
        stg_usnwr_diabetes_primary_pop.primary_key,
        procedure_order_result_clinical.visit_key,
        procedure_order_result_clinical.procedure_name,
        procedure_order_result_clinical.result_component_name,
        procedure_order_result_clinical.procedure_group_name,
        procedure_order_result_clinical.abnormal_result_ind,
        procedure_order_result_clinical.result_value,
        procedure_order_result_clinical.result_value_numeric,
        procedure_order_result_clinical.result_date
)

select
    proc_order.primary_key as patient_key,
    max(proc_order.result_value_numeric) as result_value_numeric,
    max(proc_order.abnormal_result_ind) as abnormal_result_ind,
    max(case when proc_order.result_date between (current_date - interval('2 year')) and current_date
        then 1 else 0 end) as tsh_2_yr,
    max(case when proc_order.result_date between (current_date - interval('2 year')) and current_date
        then proc_order.result_date end) as tsh_date
from
    proc_order
where
    (lower(proc_order.result_component_name) like '%tsh%'
    or lower(proc_order.result_component_name) like '%thyroid%stimulat%horm%')
    and lower(proc_order.result_value) not in (
        'tnp', 'dnr', 'canceled', 'not done', 'test not performed', 'reported in error',
        'not done; reported on this patient in error by laboratory',
        'test not performed and resulted in error by laboratory.',
        'unacceptable', 'insuff quant'
    ) -- not performed
group by
    proc_order.primary_key
