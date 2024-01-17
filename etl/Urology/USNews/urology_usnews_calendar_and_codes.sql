with submission_year as (
    select distinct
        usnews_code_list.submission_start_year as submission_year
    from
        {{ref('usnews_code_list')}} as usnews_code_list
    union
    select distinct
        usnews_code_list.submission_end_year as submission_year
    from
        {{ref('usnews_code_list')}} as usnews_code_list
    where
        usnews_code_list.submission_end_year is not null
),
urology_code_list as (
    select
        usnews_code_list.question_number,
        usnews_code_list.code_type,
        usnews_code_list.code,
        usnews_code_list.code_description,
        usnews_code_list.inclusion_ind,
        usnews_code_list.exclusion_ind,
        submission_year.submission_year
    from
        {{ref('usnews_code_list')}} as usnews_code_list
        inner join submission_year
            on submission_year.submission_year
                between usnews_code_list.submission_start_year
                and coalesce(usnews_code_list.submission_end_year, year(current_date) + 1)
    where
        usnews_code_list.division = 'Urology'
    --code list was never updated to final version in 2020. this corrects it
    union all
    select
        'k15c' as question_number,
        'CPT_CODE' as code_type,
        '50948' as code,
        'missing from cdw' as code_description,
        1 as inclusion_ind,
        0 as exclusion_ind,
        2020 as submission_year
),
final_submission_year as (
    select
        max(submission_year) as final_submission_year
    from
        submission_year
),
final_year_code_list as (
    select
        question_number,
        code_type,
        code,
        code_description,
        inclusion_ind,
        exclusion_ind,
        final_submission_year.final_submission_year + 1 as submission_year
    from
        {{ref('usnews_code_list')}} as usnews_code_list
        cross join final_submission_year
    where
        usnews_code_list.division = 'Urology'
        and submission_end_year is null
),
full_code_list as (
    select
        urology_code_list.question_number,
        urology_code_list.code_type,
        urology_code_list.code,
        urology_code_list.code_description,
        urology_code_list.inclusion_ind,
        urology_code_list.exclusion_ind,
        urology_code_list.submission_year
    from
        urology_code_list
    union all
    select
        final_year_code_list.question_number,
        final_year_code_list.code_type,
        final_year_code_list.code,
        final_year_code_list.code_description,
        final_year_code_list.inclusion_ind,
        final_year_code_list.exclusion_ind,
        final_year_code_list.submission_year
    from
        final_year_code_list
)
select
    urology_usnews_calendar.submission_year,
    urology_usnews_calendar.question,
    urology_usnews_calendar.start_date,
    urology_usnews_calendar.end_date,
    full_code_list.code_type,
    full_code_list.code,
    full_code_list.code_description,
    full_code_list.inclusion_ind,
    full_code_list.exclusion_ind,
    case when lower(full_code_list.code_description) like '%chart review%' then 1 else 0 end as chart_review_ind
from
    {{ source('manual_ods', 'urology_usnews_calendar') }} as urology_usnews_calendar
    inner join full_code_list
        on urology_usnews_calendar.code_list_qn = full_code_list.question_number
        and urology_usnews_calendar.submission_year = full_code_list.submission_year
