select
    stg_pfex_deduplicate.question_id,
    case
        when (lower(stg_pfex_deduplicate.question_id) like 'ch_%'
            or lower(stg_pfex_deduplicate.question_id) like 'cms_%')
            and stg_pfex_deduplicate.response_text in ('7', '8', '9')
            then '0 to 10'
        when (lower(stg_pfex_deduplicate.question_id) not like 'ch_%'
            and lower(stg_pfex_deduplicate.question_id) not like 'cms_%')
            and stg_pfex_deduplicate.response_text in (
                '1', '2', '3', '4', '5')
            then '1 to 5'
        when lower(stg_pfex_deduplicate.response_text) = 'always'
            then 'Never-Sometimes-Usually-Always'
        when lower(stg_pfex_deduplicate.response_text) = 'definitely no'
            then 'Definitely no-Probably no-Probably yes-Definitely yes'
        when lower(stg_pfex_deduplicate.response_text) = 'strongly agree'
            then 'Strongly disagree-Disagree-Agree-Strongly Agree'
        when lower(stg_pfex_deduplicate.response_text) = 'yes, definitely'
            then 'No-Yes, Somewhat-Yes, Definitely'
        when lower(stg_pfex_deduplicate.response_text) = 'yes' then 'No-Yes'
        when lower(stg_pfex_deduplicate.response_text) = 'checked'
            then 'Unchecked-Checked'
        when lower(stg_pfex_deduplicate.question_id) = 'cms_20'
            then 'Another facility-Another home-Own home'
        when lower(stg_pfex_deduplicate.question_id) like 'float%'
            then 'Comment'
        when lower(stg_pfex_deduplicate.question_id) like 'open%'
            then 'Comment'
        when lower(stg_pfex_deduplicate.question_id) like 'sect%'
            then 'Comment'
        else null end as response_type
from
    {{ref('stg_pfex_deduplicate')}} as stg_pfex_deduplicate
where
    /*only want one type of answer per question_id*/
    /*strip out null before grouping*/
    response_type is not null --noqa: L028
group by
    stg_pfex_deduplicate.question_id,
    response_type --noqa: L028
