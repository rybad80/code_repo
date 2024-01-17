{{ config(meta = {
    'critical': true
}) }}

select
    provider_availability.prov_key,
    provider_availability.dept_key,
    case
        when provider_availability.visit_key = -1 then 0
        else provider_availability.visit_key
    end as visit_key,
    available_block.appt_block_key,
    provider_availability.slot_start_tm,
    provider_availability.appt_num,
    provider_availability.appt_num_reg,
    provider_availability.dict_slot_unavail_rsn_key,
    provider_availability.dict_tm_held_rsn_key,
    provider_availability.slot_day_unavail_ind,
    provider_availability.slot_tm_unavail_ind,
    provider_availability.slot_lgth_min,
    available_block.block,
    provider_availability.create_dt
from
    {{ source('cdw', 'provider_availability') }} as provider_availability
left join {{ ref('stg_scheduling_available_block') }} as available_block
    on provider_availability.dept_key = available_block.dept_key
    and provider_availability.prov_key = available_block.prov_key
    and provider_availability.slot_start_tm = available_block.slot_begin_dt
where
    {{ limit_dates_for_dev(ref_date = 'provider_availability.slot_start_tm') }}
