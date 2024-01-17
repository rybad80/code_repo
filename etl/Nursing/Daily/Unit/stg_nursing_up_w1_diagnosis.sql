{{
  config(
    meta = {
      'critical': false
    }
  )
}}

/* stg_nursing_up_w1_diagnosis
capturing primary, admit & other diagnoses over time and metrics for currently
admitted patients for primary and other
*/

with enc_diagnosis as (
    select
        visit_key,
        encounter_date,
        diagnosis_name,
        visit_primary_ind,
        visit_diagnosis_ind, /*usually 1 for visit diagnosis rows*/
        visit_diagnosis_seq_num,
        marked_primary_ind,
        diagnosis_id,
        external_diagnosis_id,
        currently_admitted_ind,
        ip_admit_primary_ind,
        ip_admit_other_ind,
        admission_dept_key,
        discharge_dept_key
    from
        {{ ref('stg_nursing_up_p1_enc_diagnosis') }}
    where
        visit_diagnosis_ind = 1
),

union_set as (

    select
        'EncPrimDx' as metric_abbreviation,
        visit_key,
        discharge_dept_key as department_key,
        encounter_date as metric_date,
        sum(visit_diagnosis_ind) as numerator,
        diagnosis_id
    from
        enc_diagnosis
    where
        marked_primary_ind = 1
    group by
        visit_key,
        discharge_dept_key,
        encounter_date,
        diagnosis_id

    union all

    /* 'EncAdmitDx to grab all diagnoses that are marked as being primary admit diagnosis */
     select
        'EncAdmitDx' as metric_abbreviation,
        visit_key,
        discharge_dept_key as department_key,
        encounter_date as metric_date,
        sum(visit_diagnosis_ind) as numerator,
        diagnosis_id
    from
        enc_diagnosis
    where
        ip_admit_primary_ind = 1
    group by
        visit_key,
        discharge_dept_key,
        encounter_date,
        diagnosis_id

    union all

    select
        'EncOthDx' as metric_abbreviation,
        visit_key,
        admission_dept_key as department_key,
        current_date - 1 as metric_date,
        sum(visit_diagnosis_ind) as numerator,
        diagnosis_id
    from
        enc_diagnosis
    where
        ip_admit_other_ind = 1
    group by
        visit_key,
        admission_dept_key,
        diagnosis_id

    union all

    select
        'CurEncPrimDx' as metric_abbreviation,
        visit_key,
        admission_dept_key as department_key,
        current_date - 1 as metric_date,
        sum(visit_diagnosis_ind) as numerator,
        diagnosis_id
    from
        enc_diagnosis
    where
        currently_admitted_ind = 1
        and marked_primary_ind = 1
    group by
        visit_key,
        admission_dept_key,
        diagnosis_id

    union all

/* grab all diagnoses that are marked as being primary admit diagnosis for Current Admissions only */
    select
        'CurEncAdmitDx' as metric_abbreviation,
        visit_key,
        admission_dept_key as department_key,
        current_date - 1 as metric_date,
        sum(visit_diagnosis_ind) as numerator,
        diagnosis_id
    from
        enc_diagnosis
    where
        currently_admitted_ind = 1
        and ip_admit_primary_ind = 1
    group by
        visit_key,
        admission_dept_key,
        diagnosis_id

    union all

/* and other than primary admit diagnosis for Current Admissions only */
    select
        'CurEncOthDx' as metric_abbreviation,
        visit_key,
        admission_dept_key as department_key,
        current_date - 1 as metric_date,
        sum(visit_diagnosis_ind) as numerator,
        diagnosis_id
    from
        enc_diagnosis
    where
         currently_admitted_ind = 1
        and ip_admit_other_ind = 1
    group by
        visit_key,
        admission_dept_key,
        diagnosis_id
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
