/* stg_nursing_engage_p2_question_aggregate
Aggregate by select RN excellence questions for the All RNs nurse cohort (& its subsets) and
for the nursing unit groups the:
score & favorable, neutral, and unfavorable totals with number of respondents
only for respondent records of registered nurses
from the employee survey demogrpahic table(s) from CHOP employee engagement
assigning unit group when more than five respondents are available
*/

with for_question as (
    select
        lookup_dimension.survey_dimension_id,
        lookup_dimension.survey_dimension_abbreviation,
        lookup_question.survey_question_id,
        lookup_question.survey_question,
        lookup_question.survey_question_tdl_phrased
    from
        {{ ref('lookup_nursing_survey_dimension_nursing_question') }} as lookup_question
        inner join {{ ref('lookup_nursing_survey_dimension') }} as lookup_dimension
            on lookup_question.survey_dimension_abbreviation = lookup_dimension.survey_dimension_abbreviation
    where
        lookup_dimension.survey_dimension_id != 201 -- other question
/*     order by survey_dimension_abbreviation, survey_question */
),

tdl_demo_survey as (  -- demographic survey source
    select
        survey_year,
        case
            when survey_year = 2020
            then lpad(cost_center, 5)
            else cost_center end as cost_center,
        case
            when
                registered_nurse_ind = 1
                and direct_patient_care_ind = 1
                and primary_responsibilities_category like 'Nursing%'
            then 'Direct Care RN'
            when
                registered_nurse_ind = 1
                and direct_patient_care_ind = 0
                and primary_responsibilities_category not like '%Management%'
            then '<50% Care RN'
            when
                registered_nurse_ind = 1
                and primary_responsibilities_category like '%Management%'
            then 'Nurse Mgr'
            end as nursing_group,
        registered_nurse_ind,
        direct_patient_care_ind,
        primary_responsibilities_category,
        question,
        score,
        cast(score as numeric(10, 2)) as score_numeric
    from
        {{ ref('employee_survey_demographic') }}
    where registered_nurse_ind = 1
),

assign_unit_group as (
select
    tdl_demo_survey.survey_year,
    for_question.survey_dimension_id,  /* to know which rn excellence dimension  */
    for_question.survey_question_id,
    tdl_demo_survey.cost_center,
    coalesce(get_unit_group.unit_group_id,
        get_alt_match.unit_group_id) as unit_group_id,
    coalesce(get_unit_group.unit_group_name,
        get_alt_match.unit_group_name) as unit_group_name,
    lookup_nurse_cohort.nurse_cohort_id,
    tdl_demo_survey.score_numeric,
    case
        when tdl_demo_survey.cost_center = get_unit_group.unit_group_name
        then 1
        else 0
        end as unit_group_match_ind,
    case
        when tdl_demo_survey.cost_center = get_unit_group.cost_center_name
        then 1
        else 0
        end as cost_center_match_ind,
    case
        when tdl_demo_survey.cost_center = get_alt_match.alternate_match_name
        and unit_group_match_ind = 0
        and cost_center_match_ind = 0
        then 1
        else 0
        end as alternate_match_ind
from
    tdl_demo_survey
    inner join for_question
        on for_question.survey_question_tdl_phrased = tdl_demo_survey.question
    left join {{ ref('stg_nursing_engage_p1_year_cost_center') }} as get_unit_group
        on (tdl_demo_survey.cost_center = get_unit_group.unit_group_name
            or tdl_demo_survey.cost_center = get_unit_group.cost_center_name
            or tdl_demo_survey.cost_center = get_unit_group.cost_center_id)
            and tdl_demo_survey.survey_year = get_unit_group.survey_year
    left join {{ ref('stg_nursing_engage_p1_year_cost_center') }} as get_alt_match
        on tdl_demo_survey.cost_center = get_alt_match.alternate_match_name
            and tdl_demo_survey.survey_year = get_alt_match.survey_year

    left join chop_analytics..lookup_nurse_cohort
        on tdl_demo_survey.nursing_group = lookup_nurse_cohort.nurse_cohort_short_name
)

select
    survey_year,
    survey_dimension_id,
    survey_question_id,
    unit_group_id,
    unit_group_name,
    nurse_cohort_id,

    sum(score_numeric) as total_score,
    count(score_numeric) as num_respondents, -- only count non-null responses
    count(case when score_numeric = 5
        then score_numeric end) as top_box_count,
    count(case when score_numeric in (4, 5)
        then score_numeric end) as favorable_count,
    count(case when score_numeric = 3
        then score_numeric end) as neutral_count,
    count(case when score_numeric in (1, 2)
        then score_numeric end) as unfavorable_count,
    max(unit_group_match_ind) as unit_group_match_ind,
    max(cost_center_match_ind) as cost_center_match_ind,
    max(alternate_match_ind) as alternate_match_ind

from
    assign_unit_group
group by
    survey_year,
    survey_dimension_id,
    survey_question_id,
    unit_group_id,
    unit_group_name,
    nurse_cohort_id
having num_respondents >= 5
/*order by survey_year, survey_dimension_id, survey_question_id, unit_group_id*/
