with presurvey_cahps as (
        select
        pg_survey_cahps.survey_id,
        pg_survey_cahps.varname,
        pg_survey_cahps.value,
        cast(pg_survey_cahps.varname as varchar(50)) as question_id,
        cast(upper(pg_survey_cahps.value) as varchar(50)) as response_text,
        cast(null as varchar(10000)) as comment_text,
        cast(null as varchar(50)) as comment_valence,
        case
            when
                pg_survey_cahps.value in (
                    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '10-Best hosp',
        '0-Worst hosp', '10-Best possible', '0-Worst possible')
        then '0 to 10'
        when lower(pg_survey_cahps.value) in ('never', 'sometimes', 'usually', 'always')
        then 'Never-Sometimes-Usually-Always'
        when
            lower(
                pg_survey_cahps.value
            ) in ('definitely no', 'probably no', 'probably yes', 'definitely yes')
        then 'Definitely no-Probably no-Probably yes-Definitely yes'
        when
            lower(
                pg_survey_cahps.value
            ) in ('strongly disagree', 'disagree', 'agree', 'strongly agree', 'not given meds')
        then 'Strongly disagree-Disagree-Agree-Strongly Agree'
        when lower(pg_survey_cahps.value) in ('yes, somewhat', 'yes, definitely')
        then 'No-Yes, Somewhat-Yes, Definitely'
        when lower(pg_survey_cahps.value) in ('no', 'yes')
        then 'No-Yes'
        when lower(pg_survey_cahps.value) in ('unchecked', 'checked')
        then 'Unchecked-Checked'
        when lower(pg_survey_cahps.value) in ('another facility', 'another home', 'own home')
        then 'Another facility-Another home-Own home'
        when lower(pg_survey_cahps.value) in ('poor', 'fair', 'good', 'very good', 'excellent')
        then 'Poor-Fair-Good-Very-Good-Excellent'
        when
            lower(
                pg_survey_cahps.value
            ) in (
                '<= 8th grade',
                'some high school',
                'high school grad',
                'some college',
                '4-yr coll. grad.',
                '4+ yrs college'
            )
        then 'Parental Education'
        when
            lower(
                pg_survey_cahps.value
            ) in (
                'not span/hisp/la',
                'portuguese',
                'puerto rican',
                'mex,mex amer,chi',
                'other',
                'english',
                'chinese',
                'cuban',
                'spanish',
                'russian'
            )
        then 'Misc'
        else null end as response_type,
        case
        when
            pg_survey_cahps.value in(
                '10-Best possible',
                '10-Best hosp',
                '9',
                'Definitely yes',
                'Always',
                'Yes, definitely',
                'Strongly agree'
            )
        then 1 else 0
        end as tbs_ind,
        case when lower(pg_survey_cahps.varname) like 'ch_%'
        then 1 else 0
        end as cahps_ind,
        case when lower(pg_survey_cahps.varname) like 'cms_%'
        then 1 else 0
        end as cms_ind,
        cast(0 as int) as comment_ind,
        max(pg_survey_cahps.upd_dt) as updated
    from {{source('ods', 'pg_survey_cahps')}} as pg_survey_cahps
    group by
        pg_survey_cahps.survey_id,
        pg_survey_cahps.varname,
        pg_survey_cahps.value
),
survey_cahps as (
    select
    presurvey_cahps.survey_id,
    presurvey_cahps.question_id,
    presurvey_cahps.response_text,
    presurvey_cahps.comment_text,
    presurvey_cahps.comment_valence,
    presurvey_cahps.response_type,
    presurvey_cahps.tbs_ind,
    presurvey_cahps.cahps_ind,
    presurvey_cahps.cms_ind,
    presurvey_cahps.comment_ind,
    presurvey_cahps.updated
    from presurvey_cahps
),
survey_responses as (
select
        pg_survey_responses.survey_id,
        cast(pg_survey_responses.varname as varchar(50)) as question_id,
        cast(pg_survey_responses.value as varchar(50)) as response_text,
        cast(null as varchar(10000)) as comment_text,
        cast(null as varchar(50)) as comment_valence,
        cast('1 to 5' as varchar(50)) as response_type,
        case when pg_survey_responses.value = '5' then 1 else 0
          end as tbs_ind,
          cast(0 as int) as cahps_ind,
          cast(0 as int) as cms_ind,
          cast(0 as int) as comment_ind,
        max(pg_survey_responses.upd_dt) as updated
    from {{source('ods', 'pg_survey_responses')}} as pg_survey_responses
    where pg_survey_responses.value != 6
    group by pg_survey_responses.survey_id,
        pg_survey_responses.varname,
        pg_survey_responses.value,
        cast(null as varchar(10000)),
        cast(null as varchar(50)),
        cast('1 to 5' as varchar(50)),
        case when pg_survey_responses.value = '5' then 1 else 0 end
),
survey_comments as (
    select
        pg_survey_comments.survey_id,
        cast(pg_survey_comments.varname as varchar(50)) as question_id,
        cast(null as varchar(10000)) as response_text,
        cast(upper(pg_survey_comments.value) as varchar(10000)) as comment_text,
        cast(upper(pg_survey_comments.sentiment) as varchar(50)) as comment_valence,
        cast('Comment' as varchar(50)) as response_type,
        cast('-2' as varchar(50)) as tbs_ind,
        cast(0 as int) as cahps_ind,
          cast(0 as int) as cms_ind,
          cast(1 as int) as comment_ind,
        max(pg_survey_comments.upd_dt) as updated
    from {{source('ods', 'pg_survey_comments')}} as pg_survey_comments
    where pg_survey_comments.sentiment is not null
    group by
        pg_survey_comments.survey_id,
        cast(pg_survey_comments.varname as varchar(50)),
        response_text,
        upper(pg_survey_comments.value),
        pg_survey_comments.sentiment
),
responses_out as (
    select *
    from survey_cahps
    union
    select *
    from survey_responses
    union
    select *
    from survey_comments
),
meta_serv as (
    select distinct
    pg_survey_metadata.survey_id,
    pg_survey_metadata.service
    from {{source('ods', 'pg_survey_metadata')}} as pg_survey_metadata
)
    select
        responses_out.survey_id,
        responses_out.question_id,
        meta_serv.service,
        responses_out.response_text,
        responses_out.comment_text,
        responses_out.comment_valence,
        responses_out.response_type,
        responses_out.tbs_ind,
        responses_out.cahps_ind,
          responses_out.cms_ind,
          responses_out.comment_ind,
        responses_out.updated,
    case
        when responses_out.response_type = '0 to 10'
            and responses_out.response_text not like '0%'
            and responses_out.response_text not like '10%'
            then cast(responses_out.response_text as numeric) * 0.5
        when responses_out.response_type = '0 to 10'
            and responses_out.response_text like '0%' then 0
        when responses_out.response_type = '0 to 10'
            and responses_out.response_text like '10%' then 5
        when responses_out.response_type = '1 to 5'
            then (cast(responses_out.response_text as numeric)
                - 1) * (5.0 / 4)
        when lower(
            responses_out.response_type) = 'never-sometimes-usually-always'
            and lower(responses_out.response_text) = 'never'
            then 0
        when lower(
            responses_out.response_type) = 'never-sometimes-usually-always'
            and lower(responses_out.response_text) = 'sometimes'
            then 5.0 / 3
        when lower(
            responses_out.response_type) = 'never-sometimes-usually-always'
            and lower(responses_out.response_text) = 'usually'
            then 10.0 / 3
        when lower(
            responses_out.response_type) = 'never-sometimes-usually-always'
            and lower(responses_out.response_text) = 'always'
            then 5
        when lower(responses_out.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(responses_out.response_text) = 'definitely no'
            then 0
        when lower(responses_out.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(responses_out.response_text) = 'probably no'
            then 5.0 / 3
        when lower(responses_out.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(responses_out.response_text) = 'probably yes'
            then 10.0 / 3
        when lower(responses_out.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(responses_out.response_text) = 'definitely yes'
            then 5
        when lower(responses_out.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(responses_out.response_text) = 'strongly disagree'
            then 0
        when lower(responses_out.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(responses_out.response_text) = 'disagree'
            then 5.0 / 3
        when lower(responses_out.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(responses_out.response_text) = 'agree'
            then 10.0 / 3
        when lower(responses_out.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(responses_out.response_text) = 'strongly agree'
            then 5
        when lower(responses_out.response_type)
            = 'no-yes, somewhat-yes, definitely'
            and lower(responses_out.response_text) = 'no'
            then 0
        when lower(responses_out.response_type)
            = 'no-yes, somewhat-yes, definitely'
            and lower(responses_out.response_text) = 'yes, somewhat'
            then 2.5
        when lower(
            responses_out.response_type) = 'no-yes, somewhat-yes, definitely'
            and lower(responses_out.response_text) = 'yes, definitely'
            then 5
        when lower(responses_out.response_type) in (
            'no-yes',
            'unchecked-checked',
            'another facility-another home-own home',
            'comment')
            then null
        else null
    end as mean_value
    from responses_out
    inner join meta_serv on responses_out.survey_id = meta_serv.survey_id
