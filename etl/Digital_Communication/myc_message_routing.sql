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
    stg_myc_msg_ib_routing.convo_id,
    stg_myc_msg_ib_routing.convo_line,
    stg_myc_msg_ib_routing.myc_message_id,
    stg_myc_msg_ib_routing.msg_id,
    stg_myc_msg_ib_routing.routing_thread_id,
    stg_myc_msg_ib_routing.routing_thread_line,
    stg_myc_msg_ib_routing.create_time,
    stg_myc_msg_ib_routing.sender_user_id,
    stg_myc_msg_ib_routing.pool_id,
    stg_myc_msg_ib_routing.recip_id,
    case
        when stg_myc_msg_ib_routing.sender_user_id = '483' then 'Patient'
        else sender.job_family_role
    end as sender_job_role,
    case
        when stg_myc_msg_ib_routing.recip_id = '483' then 'Patient'
        when sender_job_role = 'Patient'
            and stg_myc_msg_ib_routing.pool_id is not null then 'Pool'
        else recip.job_family_role
    end as recipient_job_role,
    stg_myc_msg_ib_routing.routing_ind,
    stg_myc_msg_ib_routing.myc_ib_routing_primary_key
from
    {{ ref('stg_myc_msg_ib_routing') }} as stg_myc_msg_ib_routing
    left join employee as sender
        on stg_myc_msg_ib_routing.sender_user_id = sender.emp_id
    left join employee as recip
        on stg_myc_msg_ib_routing.recip_id = recip.emp_id
