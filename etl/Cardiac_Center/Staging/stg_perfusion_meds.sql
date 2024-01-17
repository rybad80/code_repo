with stg_perfusion_meds as (
select
    log_key,
    meds.pat_key,
    medication_id,
    medication_name,
    generic_medication_name,
    medication_order_name,
    medication_start_date,
    administration_date,
    admin_dose,
    med_ord_key
from
    {{ref('cardiac_perfusion_medications')}} as meds
    inner join {{ref('cardiac_perfusion_surgery')}} as surgery
        on meds.visit_key = surgery.anes_visit_key
where
    medication_id in (200202683, 11976, 7528, 7931, 200200082, 5677, 145295, 200201029, 135992)
),

stg_heparin as (
select
    surgery.log_key,
    meds.pat_key,
    medication_id,
    medication_name,
    generic_medication_name,
    medication_order_name,
    medication_start_date,
    administration_date,
    admin_dose,
    med_ord_key
from
    {{ref('medication_order_administration')}}  as meds
    inner join {{ref('cardiac_perfusion_surgery')}} as surgery
        on meds.visit_key = surgery.visit_key
	inner join {{ref('cardiac_perfusion_bypass')}} as bypass
        on surgery.anes_visit_key = bypass.visit_key
    inner join {{ref('surgery_encounter_timestamps')}} as surgery_encounter_timestamps
        on surgery.log_key = surgery_encounter_timestamps.log_key
where
     medication_id = 11976
    and administration_date between in_room_date and bypass.first_bypass_start_date
)

select
    log_key,
    pat_key,
    medication_id,
    medication_name,
    generic_medication_name,
    medication_order_name,
    medication_start_date,
    administration_date,
    admin_dose,
    med_ord_key
from
    stg_perfusion_meds

union all

select
    log_key,
    pat_key,
    medication_id,
    medication_name,
    generic_medication_name,
    medication_order_name,
    medication_start_date,
    administration_date,
    admin_dose,
    med_ord_key
from
    stg_heparin
