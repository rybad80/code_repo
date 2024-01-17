{{
    config(
		materialized = 'view',
		meta = {
			'critical': true
		}
	)
}}

with mgt_chain as (
select distinct
	wk.active_ind as worker_active_ind,
	wk.worker_id,
	clst.clarity_emp_key,
	clst.prov_key,
	clst.ad_login,
	clst.workday_emp_key as emp_key,
	clst.lvl1_reporting_nm as lvl_01_reporting_nm,
	clst.lvl2_reporting_nm as lvl_02_reporting_nm,
	clst.lvl3_reporting_nm as lvl_03_reporting_nm,
	clst.lvl4_reporting_nm as lvl_04_reporting_nm,
	clst.lvl5_reporting_nm as lvl_05_reporting_nm,
	clst.lvl6_reporting_nm as lvl_06_reporting_nm,
	clst.lvl7_reporting_nm as lvl_07_reporting_nm,
	clst.lvl8_reporting_nm as lvl_08_reporting_nm,
	clst.lvl9_reporting_nm as lvl_09_reporting_nm,
	clst.lvl10_reporting_nm as lvl_10_reporting_nm
from
{{source('workday', 'worker_contact_list')}} as clst
inner join {{ref('worker')}} as wk on clst.wd_worker_id = wk.worker_id
where (wk.worker_id != '943930') or (clst.prov_key != 0)
),

mgt_chain_1 as (
select 	  *,
	'CEO' as drill_mgmt_l01,
    case when lvl_02_reporting_nm = '' then '' else '..' || lvl_02_reporting_nm	end as drill_mgmt_l02,
    case when lvl_03_reporting_nm = '' then '' else '....' || lvl_03_reporting_nm end as drill_mgmt_l03,
    case when lvl_04_reporting_nm = '' then '' else '......' || lvl_04_reporting_nm	end as drill_mgmt_l04,
	case when lvl_05_reporting_nm = '' then '' else '........' || lvl_05_reporting_nm end as drill_mgmt_l05,
	case when lvl_06_reporting_nm = '' then '' else '..........' || lvl_06_reporting_nm	end as drill_mgmt_l06,
	case when lvl_07_reporting_nm = '' then '' else '............' || lvl_07_reporting_nm end as drill_mgmt_l07,
	case when lvl_08_reporting_nm = '' then '' else '..............' || lvl_08_reporting_nm	end as drill_mgmt_l08,
	case when lvl_09_reporting_nm = '' then '' else '................' || lvl_09_reporting_nm end as drill_mgmt_l09,
	case when lvl_10_reporting_nm = '' then '' else '..................' || lvl_10_reporting_nm end as drill_mgmt_l10
from mgt_chain
),

mgt_chain_2 as (
select    *,
	case when lvl_02_reporting_nm = '' then '' else
	drill_mgmt_l01 || ' > ' || lvl_02_reporting_nm end as drill_mgmt_l02_path,
    case when lvl_03_reporting_nm = '' then '' else
	drill_mgmt_l02_path || ' > ' || lvl_03_reporting_nm end as drill_mgmt_l03_path,
    case when lvl_04_reporting_nm = '' then '' else
	drill_mgmt_l03_path || ' > ' || lvl_04_reporting_nm end as drill_mgmt_l04_path,
	case when lvl_05_reporting_nm = '' then '' else
	drill_mgmt_l04_path || ' > ' || lvl_05_reporting_nm end as drill_mgmt_l05_path,
	case when lvl_06_reporting_nm = '' then '' else
	drill_mgmt_l05_path || ' > ' || lvl_06_reporting_nm end as drill_mgmt_l06_path,
	case when lvl_07_reporting_nm = '' then '' else
	drill_mgmt_l06_path || ' > ' || lvl_07_reporting_nm end as drill_mgmt_l07_path,
	case when lvl_08_reporting_nm = '' then '' else
	drill_mgmt_l07_path || ' > ' || lvl_08_reporting_nm end as drill_mgmt_l08_path,
	case when lvl_09_reporting_nm = '' then '' else
	drill_mgmt_l08_path || ' > ' || lvl_09_reporting_nm end as drill_mgmt_l09_path,
	case when lvl_10_reporting_nm = '' then '' else
	drill_mgmt_l09_path || ' > ' || lvl_10_reporting_nm end as drill_mgmt_l10_path
from mgt_chain_1
)

select ad_login,
	clarity_emp_key,
	drill_mgmt_l01,
	drill_mgmt_l02,
	drill_mgmt_l02_path,
	drill_mgmt_l03,
	drill_mgmt_l03_path,
	drill_mgmt_l04,
	drill_mgmt_l04_path,
	drill_mgmt_l05,
	drill_mgmt_l05_path,
	drill_mgmt_l06,
	drill_mgmt_l06_path,
	drill_mgmt_l07,
	drill_mgmt_l07_path,
	drill_mgmt_l08,
	drill_mgmt_l08_path,
	drill_mgmt_l09,
	drill_mgmt_l09_path,
	drill_mgmt_l10,
	drill_mgmt_l10_path,
	emp_key,
	prov_key,
	worker_active_ind,
	worker_id,
	coalesce(
	drill_mgmt_l10_path,
	drill_mgmt_l09_path,
	drill_mgmt_l08_path,
	drill_mgmt_l07_path,
	drill_mgmt_l06_path,
	drill_mgmt_l05_path,
	drill_mgmt_l04_path,
	drill_mgmt_l03_path,
	drill_mgmt_l02_path,
	drill_mgmt_l01
	) as full_drill_mgmt_path
from mgt_chain_2
