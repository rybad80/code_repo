{{ config(materialized='table', dist='pat_enc_csn_id') }}

--This query pulls encounters where a referral was required and determines whether or not a referral
--shell was created.
select
    stg_encounter.csn as pat_enc_csn_id,
    case when stg_encounter.rfl_key != 0 then 1 else 0 end as referral_shell_ind,
    stg_encounter.rfl_req_ind as referral_req_ind
from
    {{ref('stg_encounter')}} as stg_encounter
where
    stg_encounter.encounter_date < current_date
    and stg_encounter.rfl_req_ind = 1
