select
	stg_worker_certification.worker_id,
	stg_worker_certification.certification_name,
	stg_worker_certification.certification_end_date,
	stg_worker_certification.certification_number,
	stg_worker_certification.issuing_institution_name,
	stg_worker_certification.certification_start_date,
	stg_worker_certification.reference_id,
	stg_worker_certification.worker_full_name,
	stg_worker_certification.worker_first_name,
	stg_worker_certification.worker_last_name,
	stg_worker_certification.job_profile
from
	{{ ref('stg_worker_certification') }} as stg_worker_certification
