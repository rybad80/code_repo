with pulm_stage as (
	select 'j10a' as question_number
	union
	select 'j10b' as question_number
	union
	select 'j10c' as question_number
	union
	select 'j10d' as question_number
	union
	select 'j10e' as question_number
	union
	select 'j11' as question_number
)

select
	code.submission_start_year,
	code.submission_end_year,
	'usnwr' as source, --noqa: L029
	code.division,
	pulm_stage.question_number,
	code.code_type,
	code.code,
	code.description,
	code.inclusion_ind,
	code.exclusion_ind,
	code.code_rationale
from
	{{ref('stg_usnews_code_list')}} as code
cross join
	pulm_stage
where lower(code.question_number) like '%j10%'
