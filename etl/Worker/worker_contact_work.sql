{{ config(meta = {
    'critical': true
}) }}

select
    employee_contact.worker_id,
    worker.legal_reporting_name,
    worker.preferred_reporting_name,
    worker.ad_login,
    --employment
    worker.employee_ind,
    worker.full_time_ind,
    worker.worker_type,
    worker.cost_center_name,
    worker.worker_role,
    worker.job_family,
    worker.job_family_group,
    -- contact info
    employee_contact.email_public_ind,
    employee_contact.email_address,
    manager_contact.email_address as manager_email_address,
    employee_contact.phone_public_ind,
    employee_contact.phone_device_type,
    employee_contact.full_phone_number,
    manager_contact.phone_device_type as manager_phone_device_type,
    manager_contact.full_phone_number as manager_phone_number,
    employee_contact.address_public_ind,
    employee_contact.formatted_address,
    employee_contact.address_line_1,
    employee_contact.address_line_2,
    employee_contact.city,
    employee_contact.state,
    employee_contact.zip,
    --keys
    worker.worker_wid,
    worker.manager_worker_wid
from
    {{ ref('worker') }} as worker
    inner join {{ ref('stg_worker_contact_list') }} as employee_contact
        on employee_contact.worker_wid = worker.worker_wid
    left join {{ ref('stg_worker_contact_list') }} as manager_contact
        on manager_contact.worker_wid = worker.manager_worker_wid
        and manager_contact.usage_type = 'work'
where
    employee_contact.usage_type = 'work'
    and worker.active_ind = 1
