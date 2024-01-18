with initial_stage as (
    select
        submission_start_year,
        submission_end_year,
        cast(source as varchar(250)) as source_summary,
        cast(division as varchar(250)) as division,
        cast(lower(question_number) as varchar(250)) as question_number,
        cast(code_type as varchar(250)) as code_type,
        cast(code as varchar(250)) as code,
        case
            when description = 'NA' then 'no description provided'
            else cast(description as varchar(250))
        end as code_description,
        inclusion_ind,
        exclusion_ind,
        cast(code_rationale as varchar(250)) as code_rationale,
        1 as standard_usnews_ind
    from
        {{ref('stg_usnews_code_list')}}
    where
        lower(question_number) not like '%j10%'
    union
    select
        submission_start_year,
        submission_end_year,
        cast(source as varchar(250)) as source_summary,
        cast(division as varchar(250)) as division,
        cast(lower(question_number) as varchar(250)) as question_number,
        cast(code_type as varchar(250)) as code_type,
        cast(code as varchar(250)) as code,
        case
            when description = 'NA' then 'no description provided'
            else cast(description as varchar(250))
        end as code_description,
        inclusion_ind,
        exclusion_ind,
        cast(code_rationale as varchar(250)) as code_rationale,
        1 as standard_usnews_ind
    from
        {{ref('stg_usnews_code_list_pulm')}}
    union
    select
        submission_start_year,
        submission_end_year,
        cast(source as varchar(250)) as source_summary,
        cast(division as varchar(250)) as division,
        cast(lower(question_number) as varchar(250)) as question_number,
        cast(code_type as varchar(250)) as code_type,
        cast(code as varchar(250)) as code,
        case
            when description is null or lower(description) = 'na'
                then 'no description provided'
            else cast(description as varchar(250))
        end as code_description,
        inclusion_ind,
        exclusion_ind,
        cast(code_rationale as varchar(250)) as code_rationale,
        0 as standard_usnews_ind
    from
        {{ref('lookup_usnews_custom_codes')}}
),

stage as (
    select
        submission_start_year,
        submission_end_year,
        division,
        question_number,
        code_type,
        code,
        code_description,
        case
            when max(exclusion_ind) = 1
                then 0
                else max(inclusion_ind)
            end as inclusion_ind,
        max(exclusion_ind) as exclusion_ind,
        max(standard_usnews_ind) as standard_usnews_ind
    from
        initial_stage
    group by
        submission_start_year,
        submission_end_year,
        division,
        question_number,
        code_description,
        code_type,
        code_description,
        code
),

collapse_description as (
    select
        submission_start_year,
        submission_end_year,
        division,
        question_number,
        code_type,
        code,
        /* when there are duplicate rows of question_number and code
        the logic will give priority to the one coming from usnews first*/
        row_number() over(
			partition by
				question_number,
				code,
                submission_end_year
			order by
				standard_usnews_ind desc
		) as row_order,
        group_concat(code_description) as code_description,
        inclusion_ind,
        exclusion_ind,
        standard_usnews_ind
    from
        stage
    group by
        submission_start_year,
        submission_end_year,
        division,
        question_number,
        code_type,
        code,
        inclusion_ind,
        exclusion_ind,
        standard_usnews_ind
),

deleted as (
    select
        question_number,
        code
    from
        collapse_description

    minus

    select
        question_number,
        code
    from
        {{ref('lookup_usnews_custom_codes')}}
    where
        exclusion_ind = 1
)
select
    collapse_description.submission_start_year,
    collapse_description.submission_end_year,
    collapse_description.division,
    collapse_description.question_number,
    collapse_description.code_type,
    case
        when substring(collapse_description.code from length(collapse_description.code) for 1) = ' '
            then substring(collapse_description.code from 1 for length(collapse_description.code) - 1)
            else collapse_description.code
        end as code,
    collapse_description.code_description,
    collapse_description.inclusion_ind,
    collapse_description.exclusion_ind,
    coalesce(lookup_usnews_custom_codes.code_rationale, 'standard usnwr code') as code_rationale,
    case
        when collapse_description.submission_end_year is null
            then 1
            else 0
        end as current_code_ind,
    case
        when collapse_description.submission_start_year in ('2020', '2021', '2022', '2023')
            and collapse_description.submission_end_year is not null
            then 'removed code'
        when collapse_description.submission_start_year = '2024'
            and collapse_description.submission_end_year is null
            then 'new code'
        when collapse_description.submission_start_year in ('2020', '2021', '2022', '2023')
            and collapse_description.submission_end_year is null
            then 'same code'
            else 'check'
        end as code_change_category
from
    collapse_description
left join
    {{ref('lookup_usnews_custom_codes')}} as lookup_usnews_custom_codes
    on collapse_description.division = lookup_usnews_custom_codes.division
    and collapse_description.question_number = lookup_usnews_custom_codes.question_number
    and collapse_description.code = lookup_usnews_custom_codes.code
    and collapse_description.code_type = lookup_usnews_custom_codes.code_type
    and collapse_description.standard_usnews_ind = 0
inner join
    deleted
    on collapse_description.question_number = deleted.question_number
    and collapse_description.code = deleted.code
where
    collapse_description.row_order = 1
