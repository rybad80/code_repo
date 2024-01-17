{{
  config(
    meta = {
      'critical': false
    }
  )
}}

/* stg_nursing_up_w2_prob_list
capturing problem list items for currently admitted patients
and counting the visits for each problem lists at department granularity
*/
with enc_problem as (
    select
        visit_key,
        encounter_date,
        diagnosis_name,
        diagnosis_id,
        problem_list_ind,
        currently_admitted_ind,
        admission_dept_key
    from
        {{ ref('stg_nursing_up_p1_enc_diagnosis') }}
    where
        problem_list_ind = 1
),

union_set as (

    select
        'CurProbListItem' as metric_abbreviation,
        current_date - 1 as metric_date,
        visit_key,
        admission_dept_key as department_key,
        sum(problem_list_ind) as numerator,
        diagnosis_id
    from
        enc_problem
    where
        problem_list_ind = 1
    group by
        visit_key,
        admission_dept_key,
        diagnosis_id

    union all

    select
        'CurProbListEncCount' as metric_abbreviation,
        current_date - 1 as metric_date,
        null as visit_key,
        admission_dept_key as department_key,
        count(distinct visit_key) as numerator,
        diagnosis_id
    from
        enc_problem
    where
        problem_list_ind = 1
    group by
        diagnosis_id,
        admission_dept_key
)

select
    metric_abbreviation,
    metric_date,
    visit_key,
    department_key,
    diagnosis_id,
    null as job_code,
    null as job_group_id,
    null as metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    union_set
