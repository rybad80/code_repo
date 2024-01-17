{{
  config(
    meta = {
      'critical': false
    }
  )
}}
select
    test_components.component_id,
    max(case when test_components.compon_rpt_type_c is null
        or test_components.compon_rpt_type_c != 1 then 1 else 0 end) as reported_ind
from
    {{source('clarity_ods', 'test_components')}} as test_components
group by test_components.component_id
