{{ config(meta = {
    'critical': true
}) }}

select
    worker_id,
    nursing_lvl_04_sort_num,
    nursing_lvl_05_sort_num,
    nursing_lvl_06_sort_num,
    in_cno_org,
    in_cno_org_ind,
    cno_org_display,
    in_acno_org_ind
from {{ ref('stg_worker_management_chain') }}
