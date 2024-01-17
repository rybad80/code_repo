{{ config(meta = {
    'critical': true
}) }}

select
    dim_appt_block.dim_appt_block_key as appt_block_key,
    available_block.dept_key,
    available_block.prov_key,
    available_block.slot_begin_dt,
    dim_appt_block.appt_block_nm as block
from {{ source('cdw', 'available_block') }} as available_block
left join {{ source('cdw', 'dim_appt_block') }} as dim_appt_block
    on available_block.dim_appt_block_key = dim_appt_block.dim_appt_block_key
where
    available_block.seq_num = 1
    and {{ limit_dates_for_dev(ref_date = 'available_block.slot_begin_dt') }}
