{{ config(
	materialized='table',
	dist='pat_enc_csn_id',
	meta = {
		'critical': true
	}
) }}

select
order_proc.order_proc_id as procedure_order_id,
order_proc.pat_enc_csn_id,
zc_order_status.name as order_status,
zc_order_class.name as order_class,
zc_order_priority.name as order_priority,
clarity_eap.proc_id as procedure_id,
clarity_eap.proc_name as procedure_name,
case
	when lower(clarity_eap.proc_name) like '%research%' then 1
	else 0
end as research_ind,
stg_specimen.patient_key,
stg_specimen.pat_id,
stg_specimen.specimen_id,
spec_test_rel.spec_number_rltd as specimen_number,
spec_test_rel.line as spec_test_rel_line,
stg_specimen.specimen_collected_ind,
stg_specimen.specimen_collected_datetime,
stg_specimen.specimen_source,
stg_specimen.specimen_type,
stg_specimen.blood_draw_ind,
coalesce(dim_lab_test.lab_test_key, 0) as lab_test_key,
dim_lab_test.test_id,
stg_specimen.department_key,
stg_specimen.department_id,
coalesce(dim_lab_section.lab_section_key, 0) as lab_section_key,
dim_lab_section.lab_section_id,
dim_provider.prov_id as authorizing_provider_id,
dim_provider.full_name as authorizing_provider_name,
coalesce(ordering_worker.worker_id, '0') as ordering_worker_id,
coalesce(ordering_worker.preferred_reporting_name, ordering_clarity_emp.name) as ordering_worker_name,
coalesce(ordering_worker.job_title, 'NA') as ordering_worker_job_title,
stg_specimen.collecting_worker_id,
stg_specimen.collecting_worker_name,
stg_specimen.collecting_worker_job_title,
zc_reason_for_canc.name as cancellation_reason,
spec_test_rel.spec_tst_canc_com as cancellation_comment,
coalesce(cancelling_worker.worker_id, '0') as cancelling_worker_id,
coalesce(cancelling_worker.preferred_reporting_name, cancelling_clarity_emp.name) as cancelling_worker_name
from {{source('clarity_ods', 'order_proc')}} as order_proc
inner join {{source('clarity_ods', 'spec_test_rel')}} as spec_test_rel
    on order_proc.order_proc_id = spec_test_rel.spec_tst_order_id
inner join {{source('clarity_ods', 'zc_order_class')}} as zc_order_class
	on zc_order_class.order_class_c = order_proc.order_class_c
inner join {{source('clarity_ods', 'zc_order_priority')}} as zc_order_priority
	on zc_order_priority.order_priority_c = order_proc.order_priority_c
inner join {{source('clarity_ods', 'zc_order_status')}} as zc_order_status
	on zc_order_status.order_status_c = order_proc.order_status_c
left join {{source('clarity_ods', 'zc_reason_for_canc')}} as zc_reason_for_canc
	on zc_reason_for_canc.reason_for_canc_c = spec_test_rel.spec_tst_canc_c
inner join {{source('clarity_ods', 'clarity_eap')}} as clarity_eap
	on clarity_eap.proc_id = order_proc.proc_id
left join {{source('clarity_ods', 'clarity_emp')}} as ordering_clarity_emp
	on ordering_clarity_emp.user_id = order_proc.ord_creatr_user_id
left join {{ref('stg_pathology_distinct_worker')}} as ordering_worker
	on ordering_worker.ad_login = replace(lower(ordering_clarity_emp.system_login), 'e_', '')
left join {{source('clarity_ods', 'clarity_emp')}} as cancelling_clarity_emp
	on cancelling_clarity_emp.user_id = spec_test_rel.spec_tst_canc_id
left join {{ref('stg_pathology_distinct_worker')}} as cancelling_worker
	on cancelling_worker.ad_login = replace(lower(cancelling_clarity_emp.system_login), 'e_', '')
left join {{ref('dim_provider')}} as dim_provider
	on dim_provider.prov_id = order_proc.authrzing_prov_id
inner join {{ref('stg_specimen')}} as stg_specimen
	on stg_specimen.specimen_id = spec_test_rel.specimen_id
left join {{ref('dim_lab_test')}} as dim_lab_test
	on dim_lab_test.test_id = spec_test_rel.spec_tst_id
left join {{ref('stg_ap_tests')}} as stg_ap_tests
	on stg_ap_tests.test_id = dim_lab_test.test_id
left join {{ref('lookup_lab_section_correction')}} as lookup_lab_section_correction
    on spec_test_rel.spec_tst_sec_id = lookup_lab_section_correction.section_id
        and regexp_extract(cast(spec_test_rel.spec_number_rltd as varchar(30)), '[^-]+', 1, 2)
            = lookup_lab_section_correction.specimen_number_prefix
left join {{ref('dim_lab_section')}} as dim_lab_section
    on dim_lab_section.lab_section_id
		= coalesce(lookup_lab_section_correction.corrected_section_id, spec_test_rel.spec_tst_sec_id)
where
	stg_ap_tests.test_id is null -- Excluding AP tests
