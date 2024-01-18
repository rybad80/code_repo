{{ config(materialized='table', dist='csn') }}

select
    stg_encounter_care_network.csn,
    stg_encounter_care_network.encounter_date,
    meds_rev_hx.pat_id,
    -- the last medication reviewed at the encounter
    max(case when meds_rev_hx.meds_hx_rev_csn = stg_encounter_care_network.csn
                then meds_rev_hx.meds_hx_rev_instant end) as meds_hx_rev_instant_encounter,
    -- this indicates that the medication review happened within the context of this encounter
    max(case when meds_rev_hx.meds_hx_rev_csn = stg_encounter_care_network.csn
                then 1 end) as medications_reviewed_in_encounter_ind,
    -- medication review on the service date and within the context of the encounter per standard definition
    max(case when (cast(meds_rev_hx.meds_hx_rev_instant as date) = stg_encounter_care_network.encounter_date)
                    and (meds_rev_hx.meds_hx_rev_csn = stg_encounter_care_network.csn)
        then 1 end) as medications_reviewed_ind
from
    {{ source('clarity_ods', 'meds_rev_hx') }} as meds_rev_hx
    inner join {{ ref('stg_encounter_care_network') }} as stg_encounter_care_network
        on meds_rev_hx.pat_id = stg_encounter_care_network.pat_id
-- don't need the null like in allergy update, med_Rev_hx has no reviews outside of encounters
group by
    stg_encounter_care_network.csn,
    meds_rev_hx.pat_id,
    stg_encounter_care_network.encounter_date
