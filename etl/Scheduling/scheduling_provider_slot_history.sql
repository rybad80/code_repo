select distinct
    prov_id,
    prov_key,
    prov_type,
    prov_title,
    provider_name,
    cast(department_id as bigint) as department_id,
    dept_key,
    department_name,
    revenue_location_group,
    intended_use_id,
    pat_enc_csn_id,
    slot_begin_time,
    date(slot_begin_time) as slot_date,
    slot_lgth_min,
    appt_num_reg,
    slot_status_num,
    specialty_name,
    case when lower(specialty_name) in ('hematology', 'audiology', 'general surgery')
        then coalesce(appt_block_c, '999999999') else appt_block_c
    end as appt_block_c,
    appt_block_key,
    appt_block_nm,
    visit_type,
    unavail_reason,
    held_reason,
    case when slot_status_num not in (1, 5, 6) then 1 else 0 end as available_ind,
    case when slot_status_num in(4, 5, 7, 8) then 1 else 0 end as open_ind,
    fill_rate_incl_ind,
    tna_fill_rate_incl_ind,
    dbt_valid_from,
    dbt_valid_to,
    cast(dbt_valid_from as date) - 1 as valid_from_date,
-- Since Clarity data is always a day behind. We are correcting the dbt_valid_from_date, dbt_valid_from_date
-- to point to it.
    cast(coalesce(dbt_valid_to, current_date) as date) as valid_to_date,
    year(slot_begin_time) * 10000 + month(slot_begin_time) * 100 + day(slot_begin_time) as slot_begin_date_key,
    year(valid_from_date) * 10000 + month(valid_from_date) * 100 + day(valid_from_date) as valid_from_date_key,
    year(valid_to_date) * 10000 + month(valid_to_date) * 100 + day(valid_to_date) as valid_to_date_key,
    {{
        dbt_utils.surrogate_key([
            'department_id',
            'appt_block_c',
            'slot_lgth_min'
            ])
    }} as dept_block_lookup_key
from
    {{ref('stg_scheduling_provider_slot_history')}}
