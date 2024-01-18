{{ config(meta = {
    'critical': false
}) }}

with exclude_specialities as (
    select distinct
        department_id
    from
        {{ ref('department_care_network') }}
    where
        lower(specialty_name) in ('hematology', 'audiology', 'general surgery')
)

select
    provider_availability.prov_block_avail_id,
    provider_availability.pat_enc_csn_id,
    provider_availability.visit_type,
    provider_availability.slot_begin_time,
    provider_availability.slot_lgth_min,
    date(provider_availability.dbt_valid_from) as prov_avail_dbt_valid_from,
    date(coalesce(provider_availability.dbt_valid_to, current_timestamp)) as prov_avail_dbt_valid_to,
    date(block_availability.dbt_valid_from) as block_avail_dbt_valid_from,
    date(coalesce(block_availability.dbt_valid_to, current_timestamp)) as block_avail_dbt_valid_to,
    case
        when
            date(provider_availability.dbt_valid_from) < date(block_availability.dbt_valid_from)
            then date(block_availability.dbt_valid_from)
        else date(provider_availability.dbt_valid_from)
    end as dbt_valid_from,
    case
        when
            date(coalesce(
                provider_availability.dbt_valid_to, current_timestamp
            )) < date(coalesce(
                block_availability.dbt_valid_to, current_timestamp
            )) then date(coalesce(provider_availability.dbt_valid_to, current_timestamp))
        else date(coalesce(block_availability.dbt_valid_to, current_timestamp))
    end as dbt_valid_to,
    provider_availability.manual_close_ind,
    provider_availability.filled_ind,
    block_availability.appt_block_c,
    block_availability.appt_block_nm,
    block_availability.appt_block_key,
    provider_availability.appt_num_reg,
    provider_availability.dept_key,
    provider_availability.department_id,
    provider_availability.department_name,
    provider_availability.revenue_location_group,
    provider_availability.intended_use_id,
    provider_availability.specialty_name,
    provider_availability.prov_key,
    provider_availability.provider_name,
    provider_availability.prov_id,
    provider_availability.prov_type,
    provider_availability.prov_title,
    provider_availability.unavail_reason,
    provider_availability.held_reason,
    case
        when lower(provider_availability.unavail_reason) != 'not applicable' then 1 --unavailable
        --unavailable, epic hyperspace shortcut to blocking schedule
        when provider_availability.manual_close_ind = 1 then 6 --unavailable
        --held is currently considered an open slot
        when provider_availability.pat_enc_csn_id = 0
            and lower(provider_availability.held_reason) != 'not applicable' then 5 --unavailable
        when provider_availability.pat_enc_csn_id = 0 then 8 --open
        when provider_availability.filled_ind = 0 then 4 --open
        when provider_availability.visit_type is null
            and block_availability.appt_block_nm is null then 7 --open 
        when lower(provider_availability.visit_type) != 'default' then 2 --scheduled
        when lower(block_availability.appt_block_nm) != 'default' then 3 --scheduled
        else 9
        end as slot_status_num,
    stg_fill_rate.fill_rate_incl_ind,
    stg_fill_rate.tna_fill_rate_incl_ind
from
    {{ref('stg_scheduling_provider_availability_history')}} as provider_availability
    left join {{ref('stg_scheduling_block_availability_history')}} as block_availability
        on provider_availability.prov_block_avail_id = block_availability.prov_block_avail_id
        and date(provider_availability.dbt_valid_from)
            < date(coalesce(block_availability.dbt_valid_to, current_timestamp))
        and date(coalesce(provider_availability.dbt_valid_to, current_timestamp))
            > date(block_availability.dbt_valid_from)
    left join {{ref('stg_fill_rate_ind_lookup')}} as stg_fill_rate
        on stg_fill_rate.dept_key = provider_availability.dept_key
        and stg_fill_rate.prov_key = provider_availability.prov_key
        and coalesce(stg_fill_rate.appt_block_key, -99) = coalesce(block_availability.appt_block_key, -99)
where
(
    block_availability.prov_block_avail_id is not null
    or provider_availability.department_id in (select department_id from exclude_specialities)
)
