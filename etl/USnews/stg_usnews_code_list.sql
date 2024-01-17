with stage as (
    select
        service,
        question,
        code_type,
        case
            when lower(code_type) = 'icd10_procedure_code'
                then lpad(replace(
                    code, '''', '' --noqa: PRS
                ), 7, '0') --removing apostrophe from csv transfer
                else code
            end as code,
        description,
        case
            when lower(question) like '%_exc%'
                then 0
                else 1
            end as inclusion_ind,
        case
            when lower(question) like '%_exc%'
                then 1
                else 0
            end as exclusion_ind,
        1 as survey_year_2020_ind,
        0 as survey_year_2022_ind,
        0 as survey_year_2023_ind,
        0 as survey_year_2024_ind
    from
        {{ref('lookup_usnews_code_list_raw_2020')}}
    union distinct
    select
        service,
        question,
        code_type,
        case
            when lower(code_type) = 'icd10_procedure_code'
                then lpad(
                    replace(code, '''', '' --noqa: PRS
                ), 7, '0') --removing apostrophe from csv transfer
                else code
            end as code,
        description,
        case
            when lower(question) like '%_exc%'
                then 0
                else 1
            end as inclusion_ind,
        case
            when lower(question) like '%_exc%'
                then 1
                else 0
            end as exclusion_ind,
        0 as survey_year_2020_ind,
        1 as survey_year_2022_ind,
        0 as survey_year_2023_ind,
        0 as survey_year_2024_ind
    from
        {{ref('lookup_usnews_code_list_raw_2022')}}
    union distinct
    select
        service,
        question,
        code_type,
        case
            when lower(code_type) = 'icd10_procedure_code'
                then lpad(
                    replace(code, '''', '' --noqa: PRS
                ), 7, '0') --removing apostrophe from csv transfer
                else code
            end as code,
        description,
        case
            when lower(question) like '%_exc%'
                then 0
                else 1
            end as inclusion_ind,
        case
            when lower(question) like '%_exc%'
                then 1
                else 0
            end as exclusion_ind,
        0 as survey_year_2020_ind,
        0 as survey_year_2022_ind,
        1 as survey_year_2023_ind,
        0 as survey_year_2024_ind
    from
        {{ref('lookup_usnews_code_list_raw_2023')}}
    where
        lower(status) in ('new', 'no change')
    union distinct
    select
        service,
        question,
        code_type,
        case
            when lower(code_type) = 'icd10_procedure_code'
                then lpad(
                    replace(code, '''', '' --noqa: PRS
                ), 7, '0') --removing apostrophe from csv transfer
                else code
            end as code,
        description,
        case
            when lower(question) like '%_exc%'
                then 0
                else 1
            end as inclusion_ind,
        case
            when lower(question) like '%_exc%'
                then 1
                else 0
            end as exclusion_ind,
        0 as survey_year_2020_ind,
        0 as survey_year_2022_ind,
        0 as survey_year_2023_ind,
        1 as survey_year_2024_ind
    from
        {{ref('lookup_usnews_code_list_raw_2024')}}
    where
        lower(status) in ('new', 'no change', 'added')
),

final_stage as (
    select
        service,
        case
            when lower(question) = 'k15a_repair'
                then 'k15a1'
            when lower(question) = 'k15a_revision'
                then 'k15a2'
            when lower(question) = 'k15b_repair'
                then 'k15b1'
            when lower(question) = 'k15b_revision'
                then 'k15b2'
            when lower(question) like '%\_%'
                then substring(question from 1 for (strpos(question, '_') - 1)) --noqa: PRS,L017
                else question
        end as stage_question,
        case
            when lower(question) like '%_exc%'
                then 0
                else 1
            end as inclusion_ind,
        case
            when lower(question) like '%_exc%'
                then 1
                else 0
        end as exclusion_ind,
        question,
        code_type,
        replace(code, chr(160), '') as code,
        description,
        max(survey_year_2020_ind) as survey_year_2020_ind,
        max(survey_year_2022_ind) as survey_year_2022_ind,
        max(survey_year_2023_ind) as survey_year_2023_ind,
        max(survey_year_2024_ind) as survey_year_2024_ind,
        'usnwr' as source, --noqa: L029
        'standard usnwr code' as code_rationale
    from
        stage
    group by
        service,
        question,
        code_type,
        code,
        description
)

select
    case
        when max(survey_year_2020_ind) = 1
            then '2020'
        when max(survey_year_2022_ind) = 1
            then '2022'
        when max(survey_year_2023_ind) = 1
            then '2023'
        else '2024'
    end as submission_start_year,
    case
        when max(survey_year_2024_ind) = 1
            then null
        when max(survey_year_2023_ind) = 1
            then '2023'
        when max(survey_year_2020_ind) = 1 and max(survey_year_2022_ind) = 0
            then '2021'
            else '2022'
        end as submission_end_year,
    service as division,
    case
        when lower(stage_question) like '%dx%'
            then substring(stage_question from 1 for (strpos(stage_question, 'DX') - 1)) --noqa: PRS,L017
            when lower(stage_question) like '%proc%'
            then substring(stage_question from 1 for (strpos(stage_question, 'PROCS') - 1)) --noqa: PRS,L017
        else stage_question
    end as question_number,
    code_type,
    code,
    description, --noqa: L029
    inclusion_ind,
    exclusion_ind,
    max(survey_year_2020_ind) as survey_year_2020_ind,
    max(survey_year_2022_ind) as survey_year_2022_ind,
    max(survey_year_2023_ind) as survey_year_2023_ind,
    max(survey_year_2024_ind) as survey_year_2024_ind,
    'usnwr' as source, --noqa: L029
    'standard usnwr code' as code_rationale
from
	final_stage
group by
    service,
    question_number,
    code_type,
    code,
    inclusion_ind,
    exclusion_ind,
    description --noqa: L029
