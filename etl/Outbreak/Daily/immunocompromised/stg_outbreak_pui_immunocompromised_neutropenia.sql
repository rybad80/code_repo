{{ config(materialized='table', dist='pat_key') }}

select
    procedure_order_result_clinical.pat_key,
	cohort.outbreak_type,
	'Neutropenia' as reason,
	procedure_order_result_clinical.result_date as start_date,
	procedure_order_result_clinical.result_date + cast('1 month' as interval) as end_date,
	procedure_order_result.rslt_val as reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_cohort') }} as cohort
    inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
        on cohort.pat_key = procedure_order_result_clinical.pat_key
    inner join {{source('cdw', 'procedure_order_result')}} as procedure_order_result
        on procedure_order_result.proc_ord_key = procedure_order_result_clinical.proc_ord_key
        and procedure_order_result.seq_num = procedure_order_result_clinical.result_seq_num
    inner join {{source('cdw', 'cdw_dictionary')}} as d1
        on d1.dict_key = procedure_order_result.dict_abnorm_cd_key
where
	upper(procedure_order_result_clinical.result_component_name) like '%NEUTROP%'
	and upper(procedure_order_result_clinical.result_component_name) like '%ABSOLUTE%'
	and d1.src_id in (4, 6, 8, 19, 22, 24)
	and (procedure_order_result.rslt_num_val < 1500 or procedure_order_result.rslt_num_val = 9999999)
	and procedure_order_result_clinical.result_date between '2020-01-01'
	and (cohort.min_specimen_taken_date + interval '30 days')
group by
    procedure_order_result_clinical.pat_key,
	cohort.outbreak_type,
	reason,
	start_date,
	end_date,
	reason_detail
