{{ config(materialized='table', dist='csn') }}

select
    stg_encounter_care_network.csn,
    stg_encounter_care_network.encounter_date,
    patient_alg_upd_hx.pat_id,
    max(case when patient_alg_upd_hx.alrg_hx_rev_ept_csn = stg_encounter_care_network.csn
                then patient_alg_upd_hx.alrg_updt_dttm
        end) as alrg_updt_dttm_encounter, -- last timestamp associated with encounter
    -- this indicates that the allergy update happened within the context of encounter
    max(case when patient_alg_upd_hx.alrg_hx_rev_ept_csn = stg_encounter_care_network.csn
                then 1 end) as allergies_verified_in_encounter_ind,
    -- this indicates that the allergy was verified on the service date of  encounter
    max(case when cast(patient_alg_upd_hx.alrg_updt_dttm as date) = stg_encounter_care_network.encounter_date
                then 1 end) as allergies_verified_ind
from
    {{ source('clarity_ods', 'patient_alg_upd_hx') }} as patient_alg_upd_hx
    inner join {{ ref('stg_encounter_care_network') }} as stg_encounter_care_network
        on patient_alg_upd_hx.pat_id = stg_encounter_care_network.pat_id
where
    patient_alg_upd_hx.alrg_hx_rev_ept_csn is not null -- allergies verfied outside of an encounter
group by
    stg_encounter_care_network.csn,
    patient_alg_upd_hx.pat_id,
    stg_encounter_care_network.encounter_date
