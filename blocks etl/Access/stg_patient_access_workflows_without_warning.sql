{{ config(materialized='table', dist='pat_enc_csn_id') }}

--This query returns results at the encounter level about the number of registration workflows that occured
--during that visit and the number of those workflows where zero warnings were bypassed.
with
--The code starts by counting the number of bypassed warnings per workflow.  
count_bypassed_warnings as (
select
    reg_workflow_prod.wkfl_type_c,
    reg_workflow_prod.pat_enc_csn_id,
    coalesce(
        count(
            reg_bypassed_warnings.wkfl_type_c), 0) as count_bypassed_warnings
from
    {{source('clarity_ods', 'reg_workflow_prod')}} as reg_workflow_prod
        left join {{source('clarity_ods', 'reg_bypassed_warnings')}} as reg_bypassed_warnings
            on reg_bypassed_warnings.pat_enc_csn_id = reg_workflow_prod.pat_enc_csn_id
            and reg_bypassed_warnings.wkfl_type_c = reg_workflow_prod.wkfl_type_c
where
    reg_workflow_prod.contact_date < current_date
group by
    reg_workflow_prod.wkfl_type_c,
    reg_workflow_prod.pat_enc_csn_id
),
--Next it determines those workflows where there were zero warnings bypassed.
all_workflows as (
select
    reg_workflow_prod.pat_enc_csn_id,
    reg_workflow_prod.wkfl_type_c,
    count_bypassed_warnings.count_bypassed_warnings,
    case when
            count_bypassed_warnings = 0
            then 1 end as no_warnings
from
    {{source('clarity_ods', 'reg_workflow_prod')}} as reg_workflow_prod
        left join count_bypassed_warnings
            on count_bypassed_warnings.pat_enc_csn_id = reg_workflow_prod.pat_enc_csn_id
            and count_bypassed_warnings.wkfl_type_c = reg_workflow_prod.wkfl_type_c
where
    contact_date < current_date
)
--Finally it determines at the visit level how many workflows occurred and what number of those occurred
--without any bypassed warnings.
select
    pat_enc_csn_id,
    count(wkfl_type_c) as workflow_count,
    count(case when no_warnings = 1 then 1 end) as workflow_no_warning_count
from
    all_workflows
group by
    pat_enc_csn_id
