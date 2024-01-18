{{
  config(
    meta = {
      'critical': true
    }
  )
}}
select
    test_sections_rm.test_id
from {{source('clarity_ods', 'test_sections_rm')}} as test_sections_rm
where
    test_sections_rm.auth_lab_sec_id = 123013
