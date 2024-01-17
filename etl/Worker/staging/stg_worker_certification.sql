{{ config(meta = {
    'critical': false
}) }}

with source_certifications as (
	select
		*
	from --noqa: PRS
		{{ source('workday_ods', 'cr_workday_certifications') }}
),
final as (
	select
		wd.employee_id as worker_id,
		wd.certification as certification_name,
		wd.expiration_date as certification_end_date,
		wd.cert_number as certification_number,
		wd.issuer as issuing_institution_name,
		wd.issued_date as certification_start_date,
		wd.referenceid as reference_id,
		wd.worker as worker_full_name,
		wd.first_name as worker_first_name,
		wd.last_name as worker_last_name,
		wd.job_profile as job_profile
	from
		source_certifications as wd
)
select
	*
from
	final
