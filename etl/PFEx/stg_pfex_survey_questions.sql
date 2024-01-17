with all_questions as (
    -- region for combining duplicate questions per question_id in each 
    -- survey line post target survey implementation
    select
        stg_pfex_deduplicate.question_id,
        stg_pfex_deduplicate.question_name,
        stg_pfex_deduplicate.survey_line_id,
        stg_pfex_deduplicate.survey_sent_date,
        row_number() over(
            partition by
                stg_pfex_deduplicate.question_id,
                stg_pfex_deduplicate.survey_line_id
            order by
                stg_pfex_deduplicate.survey_sent_date desc
        ) as seq_num
    from
        {{ref('stg_pfex_deduplicate')}} as stg_pfex_deduplicate
    group by
        stg_pfex_deduplicate.question_id,
        stg_pfex_deduplicate.question_name,
        stg_pfex_deduplicate.survey_line_id,
        stg_pfex_deduplicate.survey_sent_date
-- end region 
)

select
    stg_pfex_deduplicate.survey_key,
    all_questions.*
from
    all_questions
inner join {{ref('stg_pfex_deduplicate')}} as stg_pfex_deduplicate
    on stg_pfex_deduplicate.question_id = all_questions.question_id
        and stg_pfex_deduplicate.survey_line_id = all_questions.survey_line_id
where
    all_questions.seq_num = 1
