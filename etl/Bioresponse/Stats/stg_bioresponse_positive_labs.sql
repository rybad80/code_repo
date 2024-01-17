-- aggregate to one test per day

with one_test_per_patient_day as (
    select
        bioresponse_lab_results.placed_date::date as stat_date,
        bioresponse_lab_results.patient_key,
        bioresponse_lab_results.diagnosis_hierarchy_1,
        max(case when bioresponse_lab_results.positive_ind = 1 then 1 else 0 end) as positive_test_ind
    from
        {{ ref('bioresponse_lab_results') }} as bioresponse_lab_results
    group by
        bioresponse_lab_results.placed_date::date,
        bioresponse_lab_results.patient_key,
        bioresponse_lab_results.diagnosis_hierarchy_1
)

select
    stat_date,
    diagnosis_hierarchy_1,
    sum(positive_test_ind) as stat_numerator_val
from
    one_test_per_patient_day
where
    positive_test_ind = 1
group by
    stat_date,
    diagnosis_hierarchy_1
