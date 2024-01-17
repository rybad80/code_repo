with source_education as (
	select
		*
	from --noqa: PRS
		{{ source('workday_ods', 'cr_workday_education') }}
),
final as (
	select
		se.employee_id as worker_id,
		se.preferred_name as preferred_name,
		se.degreereferenceid as degree_id,
		se.degree as degree,
		se.schoolreferenceid as school_id,
		se.education as school_name,
		se.schoolnav as schoolnav,
		se.school_location as school_location,
		se.country as edu_country,
		se.edu_state as edu_state,
		se.degree_received as graduated,
		se.institutionnav as institution_type,
		se.field_of_study as major,
		case se.start_year
			when '1900-01-01' then null
			else substr(se.start_year, 1, 4)::integer
		end as education_start_year,
		substr(se.end_year, 1, 4)::integer as education_end_year,
		substr(se.year_degree_received, 1, 4)::integer as year_degree_received,
		/* no exact date granularity, mm-dd will always be 01-01 */
		se.gpa as gpa
	from
		source_education as se
)
select
	*
from
	final
