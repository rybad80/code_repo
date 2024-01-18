select
    log_key,
    labs.pat_key,
    procedure_name,
    result_component_name,
    result_component_external_name,
    result_date as resultdate,
	max(result_value) as resultvalue,
    max(cast(regexp_replace(result_value, '[^0-9.]', '') as double)) as resultvaluenumeric
from
     {{ref('procedure_order_result_clinical')}} as labs
     inner join {{ref('cardiac_perfusion_surgery')}} as surgery
      on labs.pat_key = surgery.pat_key
where
    result_component_name in ('ACT CLOTTING TIME CARDIAC OR', 'HCT, CARDIAC OR ISTAT, POC',
                              'CREATININE', 'HCT, ISTAT8')
    or result_component_name like ('%LACTATE%W%B%')
group by
    log_key,
    labs.pat_key,
    procedure_name,
    result_component_name,
    result_component_external_name,
    result_date
