{{ config(meta = {
    'critical': true
}) }}

select
    provider_availability.prov_key,
    provider_availability.dept_key,
    visit.visit_key,
    visit.pat_key,
    visit.enc_id as csn,
    visit.appt_stat,
    visit.dict_appt_stat_key,
    visit.appt_visit_type_key,
    visit.appt_lgth_min,
    provider_availability.appt_num,
    provider_availability.appt_num_reg,
    provider_availability.slot_start_tm,
    provider_availability.dict_slot_unavail_rsn_key,
    provider_availability.dict_tm_held_rsn_key,
    provider_availability.block,
    provider_availability.appt_block_key,
    provider_availability.slot_day_unavail_ind,
    provider_availability.slot_tm_unavail_ind,
    provider_availability.slot_lgth_min,
    case when lower(dict_slot_unavail_rsn.dict_nm) != 'not applicable' then 1
        when visit.enc_id = 0 then
            case
                when provider_availability.appt_num_reg = 0 then 6
                when lower(dict_tm_held_rsn.dict_nm) != 'not applicable' then 5
                else 8
            end
        when lower(visit.appt_stat) in ('canceled', 'no show') then 4
        when master_visit_type.visit_type_nm is null and provider_availability.block is null then 7
        when lower(master_visit_type.visit_type_nm) != 'default' then 2
        when lower(provider_availability.block) != 'default' then 3
        else 9
    end as slot_status_num,
    provider_availability.create_dt
from
    {{ ref('stg_scheduling_provider_block') }} as provider_availability
inner join {{source('cdw', 'cdw_dictionary')}} as dict_slot_unavail_rsn
    on provider_availability.dict_slot_unavail_rsn_key = dict_slot_unavail_rsn.dict_key
inner join {{source('cdw', 'cdw_dictionary')}} as dict_tm_held_rsn
    on provider_availability.dict_tm_held_rsn_key = dict_tm_held_rsn.dict_key
left join {{ source('cdw', 'visit') }} as visit
    on visit.visit_key = provider_availability.visit_key
left join {{ source('cdw', 'master_visit_type') }} as master_visit_type
    on visit.appt_visit_type_key = master_visit_type.visit_type_key
where
    {{ limit_dates_for_dev(ref_date = 'provider_availability.slot_start_tm') }}
