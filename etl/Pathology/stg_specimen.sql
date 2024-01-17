{{
  config(
    meta = {
      'critical': true
    }
  )
}}
select
spec_db_main.specimen_id,
case
	when spec_db_main.spec_dtm_collected is null then 0
	else 1
end as specimen_collected_ind,
spec_db_main.spec_dtm_collected as specimen_collected_datetime,
zc_spec_source.name as specimen_source,
zc_specimen_type.name as specimen_type,
case
	when lower(zc_specimen_type.name) like '%blood%'
		or lower(zc_spec_source.name) like '%blood%' then 1
	else 0
end as blood_draw_ind,
stg_patient_ods.patient_key,
stg_patient_ods.pat_id,
coalesce(stg_department_all.department_key, '0') as department_key,
stg_department_all.department_id,
coalesce(worker.worker_id, '0') as collecting_worker_id,
coalesce(worker.preferred_reporting_name, clarity_emp.name) as collecting_worker_name,
coalesce(worker.job_title, 'NA') as collecting_worker_job_title
from {{source('clarity_ods', 'spec_db_main')}} as spec_db_main
inner join {{ref('stg_patient_ods')}} as stg_patient_ods
    on spec_db_main.spec_ept_pat_id = stg_patient_ods.pat_id
left join {{ref('stg_department_all')}} as stg_department_all
	on stg_department_all.department_id = spec_db_main.spec_coll_dept_id
left join {{source('clarity_ods', 'clarity_emp')}} as clarity_emp
	on clarity_emp.user_id = spec_db_main.spec_coll_by_id
left join {{ref('stg_pathology_distinct_worker')}} as worker
	on worker.ad_login = replace(lower(clarity_emp.system_login), 'e_', '')
left join {{source('clarity_ods', 'zc_specimen_type')}} as zc_specimen_type
	on zc_specimen_type.specimen_type_c = spec_db_main.specimen_type_c
left join {{source('clarity_ods', 'zc_spec_source')}} as zc_spec_source
	on zc_spec_source.spec_source_c = spec_db_main.spec_source_c
where (spec_db_main.spec_qc_flag_yn is null or spec_db_main.spec_qc_flag_yn != '1')
