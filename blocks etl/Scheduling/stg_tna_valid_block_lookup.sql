select distinct
    dept_block_lookup_key,
    department_id,
    slot_lgth_min,
    appt_block_c,
    appt_block_nm,
    intended_use_id,
    specialty_name,
    department_name,
    revenue_location_group,
    case
        when lower(specialty_name) = 'adolescent' and slot_lgth_min >= 30 then 1
        when lower(specialty_name) = 'audiology' and slot_lgth_min >= 15 then 1
        -- Below is for 'NEONATAL FOLLOW UP' 
        when lower(specialty_name) = 'behavioral health services'
            and lower(department_name) like '%neonatal fol up' and slot_lgth_min >= 90 then 1
        -- Below is for BEHAVIORAL HEALTH SERVICES
        when lower(specialty_name) = 'behavioral health services'
            and (lower(appt_block_nm) like '%test%' or lower(appt_block_nm) like '%eval%'
                or lower(appt_block_nm) like '%new%' or lower(appt_block_nm) like '%intake%'
                or lower(appt_block_nm) like '%npv%') then 1
        when lower(specialty_name) = 'cardiology'
            and appt_block_c in (1, 70, 71, 72, 325, 1052) then 1
        when lower(specialty_name) = 'dermatology' and slot_lgth_min >= 30 then 1
        when lower(specialty_name) = 'endocrinology' and slot_lgth_min >= 30 then 1
        when lower(specialty_name) = 'general pediatrics'
            and appt_block_c in (3431, 804, 1048, 1008, 924, 503, 8, 890, 12, 336, 496, 411, 756, 1085, 1086,
                                    894, 409, 496) and slot_lgth_min >= 15 and slot_lgth_min <= 45 then 1
        when lower(specialty_name) = 'general surgery' and slot_lgth_min >= 30 then 1
        when lower(specialty_name) = 'hematology' and slot_lgth_min >= 30 then 1
        when lower(specialty_name) = 'metabolism' and slot_lgth_min >= 30 then 1
        when lower(specialty_name) = 'neonatology' and slot_lgth_min >= 60 then 1
        when lower(specialty_name) = 'neurosurgery' and slot_lgth_min >= 30 then 1
        when lower(department_name) like '%onc%' and lower(specialty_name) = 'oncology'
            and slot_lgth_min >= 30 then 1
        when lower(specialty_name) = 'occupational therapy'
            and appt_block_c = 497 and slot_lgth_min >= 60 then 1
        when lower(specialty_name) = 'physical therapy'
            and appt_block_c in (470, 2252) and slot_lgth_min >= 60 then 1
        when lower(specialty_name) = 'speech' and appt_block_c in (38, 39)
            and slot_lgth_min >= 90 then 1
--      (allergy, rheumatology and any similar specialty will fall into below category as we are considering
--      minimum slot length to 30min for any appointment to be scheduled)
        when lower(specialty_name) not in ('adolescent', 'audiology',
            'behavioral health services', 'cardiology', 'dermatology', 'endocrinology', 'general pediatrics',
            'general surgery', 'hematology', 'metabolism', 'neonatology', 'neurosurgery', 'oncology',
            'occupational therapy', 'physical therapy', 'speech')
            and slot_lgth_min >= 30 and appt_block_c = 1 then 1 else 0 end as valid_slot_ind
from
    {{ref('scheduling_provider_slot_history')}}
where
    -- intentionally leaving qualifiers in UPPER CASE as they are the correct words.
    (revenue_location_group in ('CHCA', 'CSA')
    or (revenue_location_group = 'MSO'
        and specialty_name in ('BEHAVIORAL HEALTH SERVICES',
                                'GENERAL PEDIATRICS'))
    or (lower(department_name) like '%onc%'
        and specialty_name = 'ONCOLOGY')
    or specialty_name in ('SPEECH',
                        'OCCUPATIONAL THERAPY',
                        'AUDIOLOGY',
                        'PHYSICAL THERAPY',
                        'HEMATOLOGY',
                        'GENERAL SURGERY'))
