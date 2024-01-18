with employee as (
    select
        employee.emp_key,
        employee.emp_id,
        case
            when dim_provider.provider_type is null
                and worker.job_family is null
                then 'Other Staff'
            when worker.job_family is null
                then dim_provider.provider_type
            when dim_provider.provider_type is null
                then worker.position_title
            else worker.job_family
        end as job_family_role
    from
        {{ ref('worker') }} as worker
        left join {{ source('cdw', 'employee') }} as employee
            on employee.emp_key = worker.clarity_emp_key
        --needed until worker gets provider_key
        left join {{source ('cdw', 'provider')}} as provider
            on provider.prov_key = worker.prov_key
        left join {{ ref('dim_provider') }} as dim_provider
            on dim_provider.prov_id = provider.prov_id
    where
        worker.termination_date is null
        or worker.termination_date >= '2021-01-01'
)

select
    stg_myc_msg_ib_fwd_items.msg_id,
    stg_myc_msg_ib_fwd_items.fwd_line,
    stg_myc_msg_ib_fwd_items.forward_by_user_id,
    fwd.job_family_role as fwd_by_job_role,
    stg_myc_msg_ib_fwd_items.forwarded_time,
    stg_myc_msg_ib_fwd_items.forward_to_msg_id,
    stg_myc_msg_ib_fwd_items.rec_line,
    stg_myc_msg_ib_fwd_items.recipient_name,
    stg_myc_msg_ib_fwd_items.registry_id as fwd_to_pool_id,
    stg_myc_msg_ib_fwd_items.recipient as fwd_to_user,
    case
        when fwd_to_pool_id = '1'  --staff 
            then fwd_rec.job_family_role
        when fwd_to_user is not null and fwd_to_user = '*' -- default 'user_id' for pool
            then 'Pool' end as fwd_to_user_job_role,
    stg_myc_msg_ib_fwd_items.forwarding_ind,
    stg_myc_msg_ib_fwd_items.myc_ib_fwd_primary_key
from
    {{ref('stg_myc_msg_ib_fwd_items') }} as stg_myc_msg_ib_fwd_items
    left join employee as fwd
        on stg_myc_msg_ib_fwd_items.forward_by_user_id = fwd.emp_id
    left join employee as fwd_rec
        on stg_myc_msg_ib_fwd_items.recipient = fwd_rec.emp_id
