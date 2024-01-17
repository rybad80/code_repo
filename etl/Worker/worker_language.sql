select
        stg_worker_language.*,
        case
        when stg_worker_language.reading_proficiency = stg_worker_language.speaking_proficiency
            and stg_worker_language.speaking_proficiency = stg_worker_language.writing_proficiency
            and stg_worker_language.writing_proficiency = stg_worker_language.comprehension_proficiency
            and stg_worker_language.comprehension_proficiency is null
            and stg_worker_language.language_name = 'English'
                then 'no proficiency in English'
        when stg_worker_language.reading_proficiency = stg_worker_language.speaking_proficiency
            and stg_worker_language.speaking_proficiency = stg_worker_language.writing_proficiency
            and stg_worker_language.writing_proficiency = stg_worker_language.comprehension_proficiency
                then 'all ' || stg_worker_language.reading_proficiency
        when stg_worker_language.reading_proficiency = stg_worker_language.speaking_proficiency
            and stg_worker_language.speaking_proficiency = stg_worker_language.writing_proficiency
            and stg_worker_language.comprehension_proficiency is null
                then 'all ' || stg_worker_language.reading_proficiency || ' with no comprehension listed'
        when null in (
                stg_worker_language.reading_proficiency,
                stg_worker_language.speaking_proficiency,
                stg_worker_language.writing_proficiency,
                stg_worker_language.comprehension_proficiency
            ) then 'mixed with NULL included'
        when 'Fluent' in (
                stg_worker_language.reading_proficiency,
                stg_worker_language.speaking_proficiency,
                stg_worker_language.writing_proficiency,
                stg_worker_language.comprehension_proficiency
            ) then 'mixed with highest as Fluent'
        when 'Proficient' in (
                stg_worker_language.reading_proficiency,
                stg_worker_language.speaking_proficiency,
                stg_worker_language.writing_proficiency,
                stg_worker_language.comprehension_proficiency
            ) then 'mixed with highest as Proficient'
        when 'Intermediate' in (
                stg_worker_language.reading_proficiency,
                stg_worker_language.speaking_proficiency,
                stg_worker_language.writing_proficiency,
                stg_worker_language.comprehension_proficiency
            ) then 'mixed with highest as Intermediate'
        when 'Beginner' in (
                stg_worker_language.reading_proficiency,
                stg_worker_language.speaking_proficiency,
                stg_worker_language.writing_proficiency,
                stg_worker_language.comprehension_proficiency
            ) then 'mixed with highest as Beginner'
        when stg_worker_language.reading_proficiency is null
            and stg_worker_language.speaking_proficiency is null
            and stg_worker_language.writing_proficiency is null
            and stg_worker_language.comprehension_proficiency is null then 'proficiency not indicated'
            else 'tbd'
        end as describe_proficiency
    from
        {{ ref('stg_worker_language') }} as stg_worker_language
    where
        /*  drop off where this case resolves to 'No'  */
        case
        /* we do want if the person has no English skills */
        when stg_worker_language.reading_proficiency = stg_worker_language.speaking_proficiency
            and stg_worker_language.speaking_proficiency = stg_worker_language.writing_proficiency
            and stg_worker_language.writing_proficiency = stg_worker_language.comprehension_proficiency
            and stg_worker_language.comprehension_proficiency is null
            and stg_worker_language.language_name = 'English'
            then 'Yes'
        /*  for non-English, if all None, do not put row into TDL  */
        when stg_worker_language.reading_proficiency = stg_worker_language.speaking_proficiency
            and stg_worker_language.speaking_proficiency = stg_worker_language.writing_proficiency
            and stg_worker_language.writing_proficiency = stg_worker_language.comprehension_proficiency
            and stg_worker_language.comprehension_proficiency is null
            then 'No'
            else 'Yes'
        end = 'Yes'
