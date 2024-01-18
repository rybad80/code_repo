{{
  config(
    meta = {
      'critical': true
    }
  )
}}
select distinct
    worker_id,
    preferred_reporting_name,
    ad_login,
    job_title
from {{ ref('worker') }}
