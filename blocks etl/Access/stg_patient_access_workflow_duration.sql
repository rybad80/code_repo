{{ config(materialized='table', dist='pat_enc_csn_id') }}

--This code brings in the workflow duration for each check-in.
select
    reg_workflow_prod.pat_enc_csn_id,
    reg_workflow_prod.wkfl_duration as emp_workflow_duration
from
    {{source('clarity_ods', 'reg_workflow_prod')}} as reg_workflow_prod
where
    reg_workflow_prod.wkfl_type_c = 2
    and reg_workflow_prod.wkfl_duration is not null
    and reg_workflow_prod.contact_date < current_date
