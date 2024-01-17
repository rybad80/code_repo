-- procedure and surgeries
with or_surg_cpt_lvl as (
	select
		dx_hx.mrn,
		dx_hx.pat_key,
		surgery_procedure.surgery_date,
		surgery_procedure.surgery_csn,
		case when regexp_like(surgery_procedure.or_procedure_name, '\bEGD\b') then '43239' --EGD
			else surgery_procedure.cpt_code end as cpt_code_fix,
		surgery_procedure.visit_key,
		cast(lookup_providers.provider_id as nvarchar(20)) as provider_id,
		initcap(surgery_procedure.primary_surgeon) as provider_name,
		max(case when lookup_providers.provider_type like 'airway team ent%' then 1
			else 0 end) as airway_ent_proc_ind,
		max(case when surgery_procedure.cpt_code = '31622'
			then 1 else 0 end) as bronchoscopy_ind, --part of micro-laryngoscopy & bronchoscopy (mlb)
		max(case when surgery_procedure.cpt_code = '31526'
			then 1 else 0 end) as microlaryngoscopy_ind, --part of micro-laryngoscopy & bronchoscopy (mlb)
		max(case when surgery_procedure.cpt_code in (
				'31640', --MICROLARYNGOSCOPY, BRONCHOSCOPY, EXCISION TRACHEAL/ SUBGLOTTIC CYST
				'31571', --MICROLARYNGOSCOPY, BRONCHOSCOPY, WITH VOCAL FOLD/CORD INJECTION, W OR W/O MICROSCOPE
				'31641', --MICROLARYNGOSCOPY, BRONCHOSCOPY, ENDOSCOPIC LYSIS OF SUBGLOTTIC WEB/SCAR
				'31622', --MICROLARYNGOSCOPY,BRONCHOSCOPY WITH STENT REMOVAL
				'31561', --MICROLARYNGOSCOPY, BRONCHOSCOPY, SUPRAGLOTTOPLASTY, WITH/WITHOUT MICROSCOPE
				'31526' --MLB
				) and (surgery_procedure.or_procedure_name like '%MICROLARYNGOSCOPY%BRONCHOSCOPY%'
					or regexp_like(surgery_procedure.or_procedure_name, '\bMLB\b')
				)
			then 1 else 0 end) as mlb_ind, --micro-laryngoscopy & bronchoscopy (mlb)
		max(case when lookup_procedures.category = 'complex airway procedure' then 1 else 0 end)
			as complex_airway_proc_ind
	from {{ref('stg_frontier_airway_dx_hx')}} as dx_hx
	inner join {{ref('surgery_procedure')}} as surgery_procedure
		on dx_hx.pat_key = surgery_procedure.pat_key
	inner join {{ref('lookup_frontier_program_providers_all')}} as lookup_providers
		on surgery_procedure.primary_surgeon = upper(lookup_providers.provider_name)
		and lookup_providers.program = 'airway'
		and lookup_providers.provider_type like 'airway team%' -- includes ENT and few GI, PUL, Speech Therapy providers
	left join {{ref('lookup_frontier_program_procedures')}} as lookup_procedures
		on surgery_procedure.cpt_code = cast(lookup_procedures.id as nvarchar(20))
		and lookup_procedures.program = 'airway'
	where surgery_procedure.case_status = 'Completed'
		and (lookup_procedures.id is not null
			or regexp_like(surgery_procedure.or_procedure_name, '\bEGD\b')
			)
	group by dx_hx.mrn,
		dx_hx.pat_key,
		surgery_procedure.surgery_date,
		surgery_procedure.surgery_csn,
		cpt_code_fix,
		surgery_procedure.visit_key,
		lookup_providers.provider_id,
		surgery_procedure.primary_surgeon
),
or_surg_cpt_lvl_trach as (--Include Tracheostomy done by any ENT providers
	select
		dx_hx.mrn,
		dx_hx.pat_key,
		surgery_procedure.surgery_date,
		surgery_procedure.surgery_csn,
		surgery_procedure.cpt_code,
		surgery_procedure.visit_key,
		p.prov_id as provider_id,
		initcap(surgery_procedure.primary_surgeon) as provider_name,
		max(case when lookup_providers.provider_type like 'airway team ent%' then 1
			else 0 end) as airway_ent_proc_ind,
		0 as bronchoscopy_ind,
		0 as microlaryngoscopy_ind,
		0 as mlb_ind,
		0 as complex_airway_proc_ind
	from {{ref('stg_frontier_airway_dx_hx')}} as dx_hx
	inner join {{ref('surgery_procedure')}} as surgery_procedure
		on dx_hx.pat_key = surgery_procedure.pat_key
		and surgery_procedure.case_status = 'Completed'
		and surgery_procedure.cpt_code in ('31601', '31600') -- 'Tracheostomy, age <2', 'Tracheostomy'
	inner join {{source('cdw', 'provider')}} as p
		on surgery_procedure.primary_surgeon = p.full_nm
		and p.active_stat = 'Active'
	inner join {{source('cdw', 'provider_specialty')}} as provider_specialty
		on p.prov_key = provider_specialty.prov_key
		and provider_specialty.spec_nm in ('OTOLARYNGOLOGY', 'OTORHINOLARYGOLOGY')
	left join {{ref('lookup_frontier_program_providers_all')}} as lookup_providers
		on surgery_procedure.primary_surgeon = upper(lookup_providers.provider_name)
		and lookup_providers.program = 'airway'
		and lookup_providers.provider_type like 'airway team ent%'
	group by dx_hx.mrn,
		dx_hx.pat_key,
		surgery_procedure.surgery_date,
		surgery_procedure.surgery_csn,
		surgery_procedure.cpt_code,
		surgery_procedure.visit_key,
		p.prov_id,
		surgery_procedure.primary_surgeon
),
ov_proc_cpt_lvl as (
	select
		ov.mrn,
		ov.pat_key,
		ov.visit_key,
		procedure_billing.service_date,
		procedure_billing.cpt_code,
		procedure_billing.provider_id,
		procedure_billing.provider_name,
		max(case when lookup_providers.provider_type like 'airway team ent%'
			then 1 else 0 end) as airway_ent_proc_ind,
		max(case when lookup_procedures.category = 'complex airway procedure' then 1 else 0 end)
			as complex_airway_proc_ind
	from {{ref('stg_frontier_airway_enc_ov')}} as ov
	inner join {{ref('procedure_billing')}} as procedure_billing
		on ov.visit_key = procedure_billing.visit_key
	inner join {{ref('lookup_frontier_program_procedures')}}  as lookup_procedures
		on procedure_billing.cpt_code = cast(lookup_procedures.id as nvarchar(20))
		and lookup_procedures.program = 'airway'
	inner join {{ref('lookup_frontier_program_providers_all')}} as lookup_providers
		on procedure_billing.provider_id = cast(lookup_providers.provider_id as nvarchar(20))
		and lookup_providers.program = 'airway'
		and lookup_providers.provider_type like 'airway team ent%'
	where procedure_billing.source_summary = 'physician billing'
	group by ov.mrn,
		ov.pat_key,
		ov.visit_key,
		procedure_billing.service_date,
		procedure_billing.cpt_code,
		procedure_billing.provider_id,
		procedure_billing.provider_name
)
select
	pat_key,
	mrn,
	cpt_code_fix as cpt_code,
	surgery_date as service_date,
	surgery_csn,
	provider_id,
	provider_name,
	visit_key,
	airway_ent_proc_ind,
	bronchoscopy_ind,
	mlb_ind,
	microlaryngoscopy_ind,
	0 as tracheostomy_ind,
	complex_airway_proc_ind
from or_surg_cpt_lvl
union all
select
	pat_key,
	mrn,
	cpt_code,
	surgery_date as service_date,
	surgery_csn,
	provider_id,
	provider_name,
	visit_key,
	airway_ent_proc_ind,
	bronchoscopy_ind,
	mlb_ind,
	microlaryngoscopy_ind,
	1 as tracheostomy_ind,
	complex_airway_proc_ind
from or_surg_cpt_lvl_trach
union all
select
	pat_key,
	mrn,
	cpt_code,
	service_date,
	null as surgery_csn,
	provider_id,
	provider_name,
	visit_key,
	airway_ent_proc_ind,
	0 as bronchoscopy_ind,
	0 as mlb_ind,
	0 as microlaryngoscopy_ind,
	0 as tracheostomy_ind,
	complex_airway_proc_ind
from ov_proc_cpt_lvl
