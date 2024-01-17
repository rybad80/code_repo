{{ config(meta = {
    'critical': true
}) }}

select
    worker_contact_list.wd_worker_id,
    worker.active_ind as worker_active_ind,
    worker.active_ind * worker.rn_job_ind as worker_active_rn_job_ind,
    worker_contact_list.workday_emp_key,
    worker_contact_list.reporting_level,
    worker_contact_list.mgr_emp_key,
    worker_contact_list.lvl1_emp_key,
    worker_contact_list.lvl2_emp_key,
    worker_contact_list.lvl3_emp_key,
    worker_contact_list.lvl4_emp_key,
    worker_contact_list.lvl5_emp_key,
    worker_contact_list.lvl6_emp_key,
    worker_contact_list.lvl7_emp_key,
    worker_contact_list.lvl8_emp_key,
    worker_contact_list.lvl9_emp_key,
    worker_contact_list.lvl10_emp_key
from {{source('workday', 'worker_contact_list')}} as worker_contact_list
inner join {{ref('worker')}} as worker on worker_contact_list.workday_emp_key = worker.workday_emp_key
where
    (worker_contact_list.wd_worker_id != '943930') or (worker_contact_list.prov_key != 0)
group by
    worker_contact_list.wd_worker_id,
    worker.active_ind,
    worker.active_ind * worker.rn_job_ind,
    worker_contact_list.workday_emp_key,
    worker_contact_list.reporting_level,
    worker_contact_list.mgr_emp_key,
    worker_contact_list.lvl1_emp_key,
    worker_contact_list.lvl2_emp_key,
    worker_contact_list.lvl3_emp_key,
    worker_contact_list.lvl4_emp_key,
    worker_contact_list.lvl5_emp_key,
    worker_contact_list.lvl6_emp_key,
    worker_contact_list.lvl7_emp_key,
    worker_contact_list.lvl8_emp_key,
    worker_contact_list.lvl9_emp_key,
    worker_contact_list.lvl10_emp_key
