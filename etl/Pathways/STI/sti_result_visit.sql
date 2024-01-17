select 
    visit_key,
    mrn,
    encounter_date,
    {{ dbt_utils.pivot(
        'STI_TEST_TYPE',
        dbt_utils.get_column_values(
            table=ref('stg_sti_result_visit'),
            column='STI_TEST_TYPE',
            default='default_column_value'),
        agg = 'max',
        then_value = 1,
        else_value = 0, 
        suffix = "_TEST_IND"
    )}},
    {{ dbt_utils.pivot(
        'STI_TEST_TYPE',
        dbt_utils.get_column_values(
            table=ref('stg_sti_result_visit'),
            column='STI_TEST_TYPE',
            default='default_column_value'),
        agg = 'max',
        then_value = 'sti_test_positive_ind',
        else_value = 0,
        suffix = "_POSITIVE_IND"
    )}}

from {{ ref('stg_sti_result_visit') }}

group by 
    visit_key,
    mrn,
    encounter_date
