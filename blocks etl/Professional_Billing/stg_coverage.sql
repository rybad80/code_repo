{{ config(materialized='table', dist='coverage_id') }}

select
    coverage.coverage_id,
    coverage.plan_id
from
    {{source('clarity_ods', 'coverage')}} as coverage
