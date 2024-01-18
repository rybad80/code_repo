{{ config(meta = {
    'critical': true
}) }}

select
    worker.management_level as worker_management_level,
    worker.legal_reporting_name,
    worker.active_ind as worker_active_ind,
    worker.worker_id,
    worker_contact_list.clarity_emp_key,
    worker_contact_list.prov_key,
    worker_contact_list.ad_login,
    worker_contact_list.workday_emp_key as emp_key,
    worker_management_chain_navigation.full_drill_mgmt_path,
    worker_management_chain_navigation.drill_mgmt_l01,
    worker_contact_list.wd_worker_id,
    worker_contact_list.reporting_level,
    worker_contact_list.reporting_chain as full_names_reporting_chain,
    worker_contact_list.emp_mgmt_chain_ceo_to_wrkr as id_reporting_chain_ceo_to_worker,
    worker_contact_list.lvl1_emp_key,
    worker_contact_list.lvl2_emp_key,
    worker_contact_list.lvl3_emp_key,
    worker_contact_list.lvl4_emp_key,
    worker_contact_list.lvl5_emp_key,
    worker_contact_list.lvl6_emp_key,
    worker_contact_list.lvl7_emp_key,
    worker_contact_list.lvl8_emp_key,
    worker_contact_list.lvl9_emp_key,
    worker_contact_list.lvl10_emp_key,
    worker_contact_list.lvl1_reporting_nm as lvl_01_reporting_nm,
    worker_contact_list.lvl2_reporting_nm as lvl_02_reporting_nm,
    worker_contact_list.lvl3_reporting_nm as lvl_03_reporting_nm,
    worker_contact_list.lvl4_reporting_nm as lvl_04_reporting_nm,
    worker_contact_list.lvl5_reporting_nm as lvl_05_reporting_nm,
    worker_contact_list.lvl6_reporting_nm as lvl_06_reporting_nm,
    worker_contact_list.lvl7_reporting_nm as lvl_07_reporting_nm,
    worker_contact_list.lvl8_reporting_nm as lvl_08_reporting_nm,
    worker_contact_list.lvl9_reporting_nm as lvl_09_reporting_nm,
    worker_contact_list.lvl10_reporting_nm as lvl_10_reporting_nm
from
    {{source('workday', 'worker_contact_list')}} as worker_contact_list
    inner join {{ref('worker')}} as worker
			on worker_contact_list.wd_worker_id = worker.worker_id
    inner join {{ref('worker_management_chain_navigation')}} as worker_management_chain_navigation
			on worker_management_chain_navigation.worker_id = worker_contact_list.wd_worker_id
where
    (worker_contact_list.wd_worker_id != '943930') or (worker_contact_list.prov_key != 0)
group by
    worker.management_level,
    worker.legal_reporting_name,
    worker.active_ind,
    worker.worker_id,
    worker_management_chain_navigation.full_drill_mgmt_path,
    worker_management_chain_navigation.drill_mgmt_l01,
    worker_contact_list.wd_worker_id,
    worker_contact_list.clarity_emp_key,
    worker_contact_list.prov_key,
    worker_contact_list.ad_login,
    worker_contact_list.workday_emp_key,
    worker_contact_list.reporting_level,
    worker_contact_list.reporting_chain,
    worker_contact_list.emp_mgmt_chain_ceo_to_wrkr,
    worker_contact_list.lvl1_emp_key,
    worker_contact_list.lvl2_emp_key,
    worker_contact_list.lvl3_emp_key,
    worker_contact_list.lvl4_emp_key,
    worker_contact_list.lvl5_emp_key,
    worker_contact_list.lvl6_emp_key,
    worker_contact_list.lvl7_emp_key,
    worker_contact_list.lvl8_emp_key,
    worker_contact_list.lvl9_emp_key,
    worker_contact_list.lvl10_emp_key,
    worker_contact_list.lvl1_reporting_nm,
    worker_contact_list.lvl2_reporting_nm,
    worker_contact_list.lvl3_reporting_nm,
    worker_contact_list.lvl4_reporting_nm,
    worker_contact_list.lvl5_reporting_nm,
    worker_contact_list.lvl6_reporting_nm,
    worker_contact_list.lvl7_reporting_nm,
    worker_contact_list.lvl8_reporting_nm,
    worker_contact_list.lvl9_reporting_nm,
    worker_contact_list.lvl10_reporting_nm
