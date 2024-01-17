with slots_all as (
    select
        provider_availability.dept_key,
        cast(department.dept_id as integer) as dept_id,
        department.dept_nm,
        provider_availability.prov_key,
        provider.prov_id,
        provider.full_nm as provider_nm,
        provider_availability.slot_start_tm,
        provider_availability.slot_lgth_min,
        dim_appt_block.appt_block_nm,
        row_number() over( partition by provider_availability.dept_key, provider_availability.prov_key
            order by slot_start_tm ) as line

    from
        {{source('cdw', 'provider_availability')}} as provider_availability
        inner join {{source('cdw', 'department')}} as department
            on provider_availability.dept_key = department.dept_key
        inner join {{source('cdw', 'location')}} as location on department.rev_loc_key = location.loc_key --noqa: L029,L016
        inner join {{source('cdw', 'provider')}} as provider on provider_availability.prov_key = provider.prov_key
        inner join {{source('cdw', 'cdw_dictionary')}} as cdw_dictionary
            on provider_availability.dict_slot_unavail_rsn_key = cdw_dictionary.dict_key
        inner join {{source('cdw', 'available_block')}} as available_block
            on provider_availability.dept_key = available_block.dept_key
                and provider_availability.prov_key = available_block.prov_key
                and provider_availability.slot_start_tm = available_block.slot_begin_dt
        left join {{source('cdw', 'dim_appt_block')}} as dim_appt_block
            on available_block.dim_appt_block_key = dim_appt_block.dim_appt_block_key

    where
        provider_availability.appt_num = 0
        and provider_availability.appt_num_sched = 0
        and provider_availability.appt_num_reg != 0
        and provider_availability.dict_slot_unavail_rsn_key = -2
        and provider_availability.dict_day_held_rsn_key = -2
        and provider_availability.dict_tm_held_rsn_key = -2
        and lower(provider.prov_type) is not null
        and lower(provider.full_nm) not like '%provider%'
        and lower(provider.full_nm) not like '%nurse%'
        and lower(provider.full_nm) not like '%study%'
        and lower(provider.full_nm) not like '% prov'
        and lower(provider.full_nm) not like '% room'
        and lower(provider.full_nm) not like '% clinic'
        and lower(provider.full_nm) not like '% shot'
        and lower(location.rpt_grp_6) = 'chca'
        and lower(department.specialty) = 'cardiology'
        and lower(provider.prov_type) = 'physician'
        and lower(appt_block_nm) in (
            'new patient',
            'electrophysiology',
            'heart failure',
            'new lipid heart',
            'pulmonary hypertension',
            'transplant'
        )
        and provider_availability.slot_start_tm between
        current_date and (current_date + 180)
),

third as (
    select
        dept_key,
        dept_id,
        dept_nm,
        prov_key,
        prov_id,
        provider_nm,
        appt_block_nm,
        slot_start_tm,
        (extract(days from slot_start_tm - current_date))
        - ((extract(days from slot_start_tm - current_date) / 7) * 2)
        - (case when extract(dow from current_date) = 1 then 1 else 0 end)
        - (case when extract(dow from slot_start_tm) = 7 then 1 else 0 end) as days_to_3rd
    from
        slots_all
    where
        line = 3
)

select
    *,
    current_date as date_searched
from
    third
