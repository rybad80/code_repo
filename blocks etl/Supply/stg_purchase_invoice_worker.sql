--get the latest record for any worker wid, removing duplicates
with workday_worker as (
    select
        worker.worker_wid,
        worker.display_name_formatted,
        row_number() over (partition by worker.worker_wid order by 1) as worker_latest_row_number
    from
        {{ ref('worker') }} as worker
)

select
    *
from
    workday_worker
where
    worker_latest_row_number = 1
