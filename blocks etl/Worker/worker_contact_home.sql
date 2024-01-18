{{ config(meta = {
    'critical': true
}) }}

select
    stg_worker_contact_list.worker_id,
    worker.legal_reporting_name,
    worker.preferred_reporting_name,
    worker.ad_login,
    worker.employee_ind,
    worker.full_time_ind,
    --contact
    stg_worker_contact_list.email_public_ind,
    stg_worker_contact_list.email_address,
    stg_worker_contact_list.phone_public_ind,
    stg_worker_contact_list.phone_device_type,
    stg_worker_contact_list.full_phone_number,
    stg_worker_contact_list.address_public_ind,
    stg_worker_contact_list.formatted_address,
    stg_worker_contact_list.address_line_1,
    stg_worker_contact_list.address_line_2,
    stg_worker_contact_list.city,
    stg_worker_contact_list.state,
    stg_worker_contact_list.zip,
    --employment 
    worker.worker_type,
    worker.job_family,
    worker.job_family_group,
    worker.position_time_type,
    worker.worker_role,
    worker.cost_center_name,
    --keys
    worker.worker_wid,
    worker.manager_worker_wid,
    stg_worker_contact_list.worker_contact_id
from
    {{ ref('worker') }} as worker
    inner join {{ ref('stg_worker_contact_list') }} as stg_worker_contact_list
        on stg_worker_contact_list.worker_wid = worker.worker_wid
where
    stg_worker_contact_list.usage_type = 'home'
    and worker.active_ind = 1
