select distinct
    or_log_implants.impl_key,
    surgery_procedure.case_key,
    or_log_implants.seq_num as or_log_seq_num,
    surgery_procedure.patient_name,
    surgery_procedure.mrn,
    stg_patient.sex,
    stg_patient.race_ethnicity,
    surgery_procedure.dob,
    surgery_procedure.csn,
    surgery_procedure.encounter_date,
    surgery_procedure.surgery_date,
    surgery_procedure.log_id,
    case
        when lower(or_supply.supply_nm) = 'default' then lower(or_implant.impl_nm)
        else lower(or_supply.supply_nm)
    end as implant_name,
    case
        when implant_name like '%screw%' or implant_name like '% scr %' or implant_name like '% scw %'
            or implant_name like '%scrw%' or implant_name like '%srew%'
            or implant_name like '% caps%' or implant_name like '% cap%'
            then 'screw/caps'
        when implant_name like '%plate%' or implant_name like '% plt %' then 'plate'
        when implant_name like '% anch%' then 'anchor'
        when implant_name like '% bar %' then 'bar'
        when implant_name like '% chip%' then 'chip'
        when implant_name like '%clamp%' or implant_name like '%clmp%' then 'clamp'
        when implant_name like '%graft%' or implant_name like '% grft %' then 'graft'
        when implant_name like '%locking%' then 'locking'
        when implant_name like '% nail%' then 'nail'
        when implant_name like '% pins %' or implant_name like '% pins' then 'pin'
        when implant_name like '% pin %' or implant_name like '% pin' then 'pin'
        when implant_name like '% ring%' then 'ring'
        when implant_name like '% rod%' then 'rod'
        when implant_name like '%staple%' then 'staple'
        when implant_name like '% tight%rope' or implant_name like '% tightrope%' then 'tightrope'
        when implant_name like '%wire%' or implant_name like '% wire%' then 'wire'
        else 'other'
    end as implant_type,
    or_implant.impl_id as implant_id,
    dict_impl_actn.dict_nm as implant_action,
    dict_impl_actn.src_id as implant_action_id,
    dict_or_impl_stat.dict_nm as current_status,
    dict_or_impl_stat.src_id as current_status_id,
    dict_impl_area.dict_nm as implant_area,
    dict_or_lateral.dict_nm as implant_laterality,
    or_log_implants.impl_num_used as implant_number_used,
    dict_or_rsn_wasted.dict_nm as reason_for_waste,
    or_implant.model_num as item_model_number,
    or_implant.unit_cost as item_unit_cost,
    dict_or_manuf.dict_nm as item_manufacturer_name,
    dict_or_manuf.src_id as item_manufacturer_id,
    dict_or_supplier.dict_nm as item_supplier_name,
    dict_or_supplier.src_id as item_supplier_id,
    or_implant.impl_sn as item_serial_number,
    surgery_procedure.pat_key,
    surgery_procedure.hsp_acct_key,
    surgery_procedure.visit_key,
    surgery_procedure.log_key,
    or_implant.supply_key
from
    {{source('cdw', 'or_log_implants')}} as or_log_implants
    inner join {{ref('surgery_procedure')}} as surgery_procedure
        on surgery_procedure.log_key = or_log_implants.log_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = surgery_procedure.pat_key
    inner join {{source('cdw', 'or_implant')}} as or_implant
        on or_implant.impl_key = or_log_implants.impl_key
    left join {{source('cdw', 'cdw_dictionary')}} as dict_impl_actn
        on dict_impl_actn.dict_key = or_log_implants.dict_impl_actn_key
        and or_log_implants.dict_impl_actn_key != -2
    left join {{source('cdw', 'cdw_dictionary')}} as dict_impl_area
        on dict_impl_area.dict_key = or_log_implants.dict_impl_area_key
        and or_log_implants.dict_impl_area_key != -2
    left join {{source('cdw', 'cdw_dictionary')}} as dict_or_lateral
        on dict_or_lateral.dict_key = or_log_implants.dict_or_lateral_key
        and dict_or_lateral.src_id in (1, 2, 3) --right, left, bilateral
    left join {{source('cdw', 'cdw_dictionary')}} as dict_or_rsn_wasted
        on dict_or_rsn_wasted.dict_key = or_log_implants.dict_or_rsn_wasted_key
        and or_log_implants.dict_or_rsn_wasted_key != -2
    left join {{source('cdw', 'or_supply')}} as or_supply
        on or_supply.supply_key = or_implant.supply_key
    left join {{source('cdw', 'cdw_dictionary')}} as dict_or_impl_stat
        on dict_or_impl_stat.dict_key = or_implant.dict_or_impl_stat_key
        and or_implant.dict_or_impl_stat_key != -2
    left join {{source('cdw', 'cdw_dictionary')}} as dict_or_manuf
        on dict_or_manuf.dict_key = or_implant.dict_or_manuf_key
        and or_implant.dict_or_manuf_key not in (-2, -1)
    left join {{source('cdw', 'cdw_dictionary')}} as dict_or_supplier
        on dict_or_supplier.dict_key = or_implant.dict_or_supplier_key
        and or_implant.dict_or_supplier_key not in (-1, 0)
where
    lower(surgery_procedure.service) = 'orthopedics'
    and or_log_implants.impl_key not in (0, -1)
