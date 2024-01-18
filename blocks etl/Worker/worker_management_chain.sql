{{ config(meta = {
    'critical': true
}) }}

select
    worker_id,
    worker_management_level,
    legal_reporting_name,
    worker_active_ind,
    clarity_emp_key,
    prov_key,
    ad_login,
    emp_key,
    full_drill_mgmt_path,
    reporting_level,
    full_names_reporting_chain,
    id_reporting_chain_ceo_to_worker,
    rank_chain_by_mgmt_level,
    sort_by_mgmt_level,
    lvl_01_reporting_nm,
    lvl_02_reporting_nm,
    lvl_03_reporting_nm,
    lvl_04_reporting_nm,
    lvl_05_reporting_nm,
    lvl_06_reporting_nm,
    lvl_07_reporting_nm,
    lvl_08_reporting_nm,
    lvl_09_reporting_nm,
    lvl_10_reporting_nm,
    lvl_03_management_level,
    lvl_04_management_level,
    lvl_05_management_level,
    lvl_06_management_level
from {{ ref('stg_worker_management_chain') }}
