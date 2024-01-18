with event_anesthesia as (
    select
        stg_pcoti_event_surgery.*
    from
         {{ ref('stg_pcoti_event_surgery') }} as stg_pcoti_event_surgery
    where
        stg_pcoti_event_surgery.event_type_abbrev = 'SURG_ANES'
)

select
    medication_order_administration.pat_key,
    medication_order_administration.visit_key,
    medication_order_administration.medication_id,
    medication_order_administration.med_ord_key,
    initcap(
        regexp_extract(
            upper(medication_order_administration.medication_order_name),
            '^EPINEPHRINE|^DOPAMINE|^NOREPINEPHRINE|^DOBUTAMINE|^MILRINONE|^PHENYLEPHRINE'
        )
    ) as medication_name,
    medication_order_administration.medication_name as medication_name_full,
    medication_order_administration.administration_date
from
    {{ ref('medication_order_administration') }} as medication_order_administration
    inner join {{ source('cdw', 'medication_order') }} as medication_order
        on medication_order_administration.med_ord_key = medication_order.med_ord_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
        on medication_order.dict_med_reord_key = cdw_dictionary.dict_key
    left join event_anesthesia
        on medication_order_administration.pat_key = event_anesthesia.pat_key
        and medication_order_administration.administration_date >= event_anesthesia.event_start_date
        and medication_order_administration.administration_date <= event_anesthesia.event_end_date
where
    regexp_like(
        upper(medication_order_administration.medication_order_name),
        '^EPINEPHRINE|^DOPAMINE|^NOREPINEPHRINE|^DOBUTAMINE|^MILRINONE|^PHENYLEPHRINE'
    )
    and event_anesthesia.pat_key is null
    and medication_order_administration.medication_frequency like 'CONTINUOUS%'
    and cdw_dictionary.src_id = -2 -- new therapies only, no reordered medications
    and medication_order_administration.administration_type_id in (
        '1',        -- GIVEN
        '102',      -- PT/CAREGIVER ADMIN - NON HIGH ALERT
        '103',      -- PT/CAREGIVER ADMIN - HIGH ALERT
        '105',      -- GIVEN BY OTHER
        '106',      -- NEW SYRINGE
        '112',      -- IV STARTED
        '115',      -- IV RESTARTED
        '116',      -- DIVIDED DOSE
        '117',      -- STARTED BY OTHER
        '119',      -- NEB RESTARTED
        '12',       -- BOLUS
        '120.0020', -- PERFORMED
        '127',      -- BOLUS FROM BAG/BOTTLE/SYRINGE
        '6',        -- NEW BAG
        '7',        -- RESTARTED
        '9'         -- RATE CHANGE
    )
