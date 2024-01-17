select
    provider_availability.prov_block_avail_id,
    case
        when provider_availability.pat_enc_csn_id = -1
            then 0 else provider_availability.pat_enc_csn_id
    end as pat_enc_csn_id,
    provider_availability.visit_type,
    provider_availability.dbt_valid_from,
    provider_availability.dbt_valid_to,
    provider_availability.slot_begin_time,
    provider_availability.slot_lgth_min,
    provider_availability.prov_id,
    provider_availability.department_id,
    provider_availability.appt_num_reg,
    provider_availability.dict_slot_unavail_rsn_c,
    provider_availability.dict_cat_slot_unavail_c,
    provider_availability.dict_slot_held_rsn_c,
    provider_availability.dict_cat_slot_held_c,
    provider_availability.manual_close_ind,
    provider_availability.filled_ind,
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.revenue_location_group,
    stg_department_all.intended_use_id,
    stg_department_all.specialty_name,
    provider.prov_key,
    provider.full_nm as provider_name,
    provider.prov_type,
    provider.title as prov_title,
    dict_slot_unavail_rsn.dict_nm as unavail_reason,
    dict_tm_held_rsn.dict_nm as held_reason,
    case when (lower(provider.prov_type) != 'resource'
                and lower(provider.full_nm) not like '%provider%'
                and lower(provider.full_nm) not like '%nurse%'
                and lower(provider.full_nm) not like '%study%'
                and lower(provider.full_nm) not like '% prov'
                and lower(provider.full_nm) not like '% room'
                and lower(provider.full_nm) not like '% clinic'
                and lower(provider.full_nm) not like '% shot'
                and lower(provider.full_nm) not like '% lab'
                and lower(provider.full_nm) not like 'transplant%'
                and lower(provider.full_nm) not like '% other'
                and stg_department_all.department_id not in (
                    92138043,
                    84187043,
                    1012107,
                    89476031,
                    101001089,
                    10270011,
                    101012136
                    )) then 1
            else 0
    end as provider_ind
from
    {{ref('provider_availability_snapshot')}} as provider_availability
inner join {{source('cdw', 'provider')}} as provider
    on provider_availability.prov_id = provider.prov_id
inner join {{source('cdw', 'cdw_dictionary')}} as dict_slot_unavail_rsn
    on provider_availability.dict_slot_unavail_rsn_c = dict_slot_unavail_rsn.src_id
    and provider_availability.dict_cat_slot_unavail_c = dict_slot_unavail_rsn.dict_cat_key
inner join {{source('cdw', 'cdw_dictionary')}} as dict_tm_held_rsn
    on provider_availability.dict_slot_held_rsn_c = dict_tm_held_rsn.src_id
    and provider_availability.dict_cat_slot_held_c = dict_tm_held_rsn.dict_cat_key
inner join {{ref('stg_department_all')}} as stg_department_all
    on stg_department_all.department_id = provider_availability.department_id
where
    provider_ind = 1
