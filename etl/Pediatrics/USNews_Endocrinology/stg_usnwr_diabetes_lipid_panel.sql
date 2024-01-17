select distinct
    stg_usnwr_diabetes_primary_pop.primary_key as patient_key,
    stg_usnwr_diabetes_primary_pop.mrn,
    procedure_order_result_clinical.visit_key,
    procedure_order_result_clinical.encounter_date,
    procedure_order_result_clinical.procedure_name,
    procedure_order_result_clinical.result_component_name,
    procedure_order_result_clinical.procedure_group_name,
    procedure_order_result_clinical.abnormal_result_ind,
    procedure_order_result_clinical.result_value,
    procedure_order_result_clinical.result_value_numeric,
    date(procedure_order_result_clinical.result_date) as result_date,
    case
        when result_date
            between (current_date - interval('1 year')) and current_date
        then 1 else 0
    end as lipid_1yr_ind,
    case
        when result_date
            between (current_date - interval('3 year')) and current_date
        then 1 else 0
    end as lipid_3yr_ind,
    row_number() over(
        partition by
            stg_usnwr_diabetes_primary_pop.primary_key
        order by
            procedure_order_result_clinical.encounter_date desc
    ) as ldl_num
from
    {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
    inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
        on procedure_order_result_clinical.pat_key = stg_usnwr_diabetes_primary_pop.pat_key
where
    --only include final lab results and final edited results
    lower(procedure_order_result_clinical.result_lab_status) in ('final result', 'edited result - final')
    --procedure_name like lipid panel
    and (lower(procedure_order_result_clinical.result_component_name) like 'ldl%'
        --exclude ldl/hdl ratio
        and lower(procedure_order_result_clinical.result_component_name) not like 'ldl/%')
    and lower(procedure_order_result_clinical.result_value) not in (
        'tnp', 'dnr', 'canceled', 'not done', 'test not performed', 'reported in error',
        'not done; reported on this patient in error by laboratory',
        'test not performed and resulted in error by laboratory.',
        'unacceptable', 'insuff quant'
    ) -- not performed
    and procedure_order_result_clinical.result_value is not null
