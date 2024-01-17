{{ config(meta = {
    'critical': false
}) }}

with source_language as (
	select
		*
	from --noqa: PRS
		{{ source('workday_ods', 'cr_workday_language_skills') }}
),
base_worker_language_fields as (
	select
		employee_id as worker_id,
		languages as language_name,
		assessed_on,
		reading as reading_proficiency,
		speaking as speaking_proficiency,
		writing_ability as writing_proficiency,
		comprehension as comprehension_proficiency
	from
		source_language
),
final as (
	select
		worker_id,
		language_name,
		assessed_on,
		case
			when reading_proficiency like '%Beginner%' then 'Beginner'
			when reading_proficiency like '%Intermediate%' then 'Intermediate'
			when reading_proficiency like '%Proficient%' then 'Proficient'
			when reading_proficiency like '%Fluent%' then 'Fluent'
			when reading_proficiency like '%none%' then 'none'
			else null
		end as reading_proficiency,
		case
			when speaking_proficiency like '%Beginner%' then 'Beginner'
			when speaking_proficiency like '%Intermediate%' then 'Intermediate'
			when speaking_proficiency like '%Proficient%' then 'Proficient'
			when speaking_proficiency like '%Fluent%' then 'Fluent'
			when speaking_proficiency like '%none%' then 'none'
			else null
		end as speaking_proficiency,
		case
			when writing_proficiency like '%Beginner%' then 'Beginner'
			when writing_proficiency like '%Intermediate%' then 'Intermediate'
			when writing_proficiency like '%Proficient%' then 'Proficient'
			when writing_proficiency like '%Fluent%' then 'Fluent'
			when writing_proficiency like '%none%' then 'none'
			else null
		end as writing_proficiency,
		case
			when comprehension_proficiency like '%Beginner%' then 'Beginner'
			when comprehension_proficiency like '%Intermediate%' then 'Intermediate'
			when comprehension_proficiency like '%Proficient%' then 'Proficient'
			when comprehension_proficiency like '%Fluent%' then 'Fluent'
			when comprehension_proficiency like '%none%' then 'none'
			else null
		end as comprehension_proficiency
	from
		base_worker_language_fields
)
select *
from
	final
