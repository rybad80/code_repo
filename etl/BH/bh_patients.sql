with

bh_patient_list as ( --region Create patient list from all other criteria
select
    pat_key
from
    {{ref('stg_bh_diagnoses')}}
union
select
    pat_key
from
    {{ref('bh_orders')}}
union
select
    pat_key
from
    {{ref('bh_screenings_summary')}}
where
    bh_screened_pos_any_ind = 1
union
select
    pat_key
from
    {{ref('bh_notes')}}
union
select
    pat_key
from
    {{ref('bh_meds')}}
union
select
    pat_key
from
    {{ref('bh_dcapbs_encounters')}}
),

-- end region

bh_patient_diagnosis as (
select distinct
    pat_key as pat_key,
    1 as bh_diagnosis_ind
from
    {{ref('stg_bh_diagnoses')}}
),

bh_patient_orders as (
select distinct
    pat_key as pat_key,
    1 as bh_orders_ind
from
    {{ref('bh_orders')}}
),

bh_patient_screenings as (
select distinct
    pat_key as pat_key,
    1 as bh_screenings_ind
from
    {{ref('bh_screenings_summary')}}
where
    bh_screened_pos_any_ind = 1
),

bh_patient_notes as (
select distinct
    pat_key as pat_key,
    1 as bh_notes_ind
from
    {{ref('bh_notes')}}
),

bh_patient_meds as (
select distinct
    pat_key as pat_key,
    1 as bh_meds_ind
from
    {{ref('bh_meds')}}
),

bh_patient_dcapbs as (
select distinct
    pat_key as pat_key,
    1 as bh_dcapbs_encounters_ind
from
    {{ref('bh_dcapbs_encounters')}}
)

select
    bh_patient_list.pat_key,
    patient_all.patient_name,
    patient_all.mrn,
    patient_all.pat_id,
    patient_all.dob,
    patient_all.sex,
    patient_all.current_age,
    patient_all.county,
    patient_all.race,
    patient_all.ethnicity,
    patient_all.race_ethnicity,
    patient_all.preferred_language,
    patient_all.preferred_name,
    patient_all.deceased_ind,
    patient_all.death_date,
    patient_all.record_state,
    patient_all.current_record_ind,
    patient_all.payor_name,
    patient_all.payor_group,
    patient_all.payor_start_date,
    patient_all.current_pcp_location,
    patient_all.current_pcp_provider,
    patient_all.mychop_activation_ind,
    coalesce(bh_patient_diagnosis.bh_diagnosis_ind, 0) as bh_diagnosis_ind,
    coalesce(bh_patient_orders.bh_orders_ind, 0) as bh_orders_ind,
    coalesce(bh_patient_meds.bh_meds_ind, 0) as bh_meds_ind,
    coalesce(bh_patient_screenings.bh_screenings_ind, 0) as bh_screenings_ind,
    coalesce(bh_patient_notes.bh_notes_ind, 0) as bh_notes_ind,
    coalesce(bh_patient_dcapbs.bh_dcapbs_encounters_ind, 0) as bh_dcapbs_encounters_ind
from
    bh_patient_list
    inner join {{ref('patient_all')}} as patient_all
        on patient_all.pat_key = bh_patient_list.pat_key
    left join
        bh_patient_diagnosis        on
            bh_patient_diagnosis.pat_key = bh_patient_list.pat_key
    left join
        bh_patient_orders           on
            bh_patient_orders.pat_key = bh_patient_list.pat_key
    left join
        bh_patient_screenings       on
            bh_patient_screenings.pat_key = bh_patient_list.pat_key
    left join
        bh_patient_notes            on
            bh_patient_notes.pat_key = bh_patient_list.pat_key
    left join
        bh_patient_meds             on
            bh_patient_meds.pat_key = bh_patient_list.pat_key
    left join
        bh_patient_dcapbs           on
            bh_patient_dcapbs.pat_key = bh_patient_list.pat_key
