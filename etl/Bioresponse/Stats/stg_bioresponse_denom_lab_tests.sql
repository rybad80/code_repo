-- aggregate to one test per day

with one_test_per_patient_day as (
    select
        bioresponse_lab_results.placed_date::date as stat_date,
        bioresponse_lab_results.patient_key,
        bioresponse_lab_results.diagnosis_hierarchy_1,
        count(bioresponse_lab_results.patient_key) as n_tests
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
    count(patient_key) as stat_denominator_val
from
    one_test_per_patient_day
group by
    stat_date,
    diagnosis_hierarchy_1
