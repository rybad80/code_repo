/* find the latest scr on or before the index date */
select distinct
    procedure_order_result_clinical.visit_key,
    stg_naki_daily.index_date,
    procedure_order_result_clinical.result_value,
    procedure_order_result_clinical.specimen_taken_date,
    row_number() over (
        partition by
            procedure_order_result_clinical.visit_key,
            stg_naki_daily.index_date
        order by procedure_order_result_clinical.specimen_taken_date desc
    ) as rn
from
    {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
    inner join {{ ref('stg_naki_daily') }} as stg_naki_daily
        on stg_naki_daily.visit_key = procedure_order_result_clinical.visit_key
        and stg_naki_daily.index_date >= procedure_order_result_clinical.specimen_taken_date
where
    lower(procedure_order_result_clinical.result_component_name) in ('creatinine', 'creatinine, serum')
