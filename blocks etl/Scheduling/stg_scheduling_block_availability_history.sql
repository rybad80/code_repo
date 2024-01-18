select
    block_availability_snapshot.block_avail_id,
    block_availability_snapshot.prov_block_avail_id,
    block_availability_snapshot.dbt_valid_from,
    block_availability_snapshot.dbt_valid_to,
    block_availability_snapshot.slot_begin_time,
    block_availability_snapshot.prov_id,
    block_availability_snapshot.department_id,
    block_availability_snapshot.rel_block_c,
    block_availability_snapshot.block_c,
    coalesce(block_availability_snapshot.block_c, block_availability_snapshot.rel_block_c) as appt_block_c,
    block_availability_snapshot.line,
    block_availability_snapshot.org_avail_blocks,
    block_availability_snapshot.blocks_used,
    dim_appt_block.appt_block_nm,
    dim_appt_block.dim_appt_block_key as appt_block_key
from
    {{ref('block_availability_snapshot')}} as block_availability_snapshot
left join {{source('cdw', 'dim_appt_block')}} as dim_appt_block
    on coalesce(block_availability_snapshot.block_c, block_availability_snapshot.rel_block_c)
         = dim_appt_block.appt_block_id
