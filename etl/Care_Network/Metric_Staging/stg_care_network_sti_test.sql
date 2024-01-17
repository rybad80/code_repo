with cohort as (
--region cohort: all female adolescent (13+) well visits (billed) at CN sites since 10/1/2022
    select
        stg_encounter_outpatient.visit_key,
        stg_encounter_outpatient.pat_key,
        stg_encounter_outpatient.sex,
        dfloor(stg_encounter_outpatient.age_years) as age_years,
        stg_encounter_outpatient.encounter_date
    from {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
    where
        stg_encounter_outpatient.primary_care_ind = 1
        and stg_encounter_outpatient.well_visit_ind = 1
        /*Encounter types of 'Office Visit', 'Appointment', 'Confidential Visit'*/
        and stg_encounter_outpatient.encounter_type_id in ('101', '50', '155')
        /*Appointment statuses of 'COMPLETED', 'ARRIVED', and 'NOT APPLICABLE'*/
        and stg_encounter_outpatient.appointment_status_id in (2, 6, -2)
        and stg_encounter_outpatient.age_years >= 13
        and stg_encounter_outpatient.age_years < 20
        and stg_encounter_outpatient.encounter_date >= '2022-07-01'
        and stg_encounter_outpatient.encounter_date < (current_date - 6)
        and stg_encounter_outpatient.department_id not in (66315012, 89296012)
        and stg_encounter_outpatient.sex = 'F'
),

adolescent_health_questionnaire as (
--region ahq: visit-level indicators for whether ahq was given and response to selected items
    select
        question_patient_answered.visit_key,
        question_patient_answered.pat_key,
        question_patient_answered.encounter_date,
        1 as ahq_given_ind,
        /*indicator for yes/maybe response to sexual contact question*/
        max(
            case
                when question_patient_answered.form_question_id = '118084'
                and (
                    question_patient_answered.answer_as_string = 'Yes'
                    or question_patient_answered.answer_as_string = 'Maybe'
                    )
                then 1
                when question_patient_answered.form_question_id = '118084'
                and question_patient_answered.answer_as_string = 'No'
                then 0
            else null
        end) as ahq_sexually_active_ind,
        /*indicator for any response to sexual contact question*/
        max(
            case
                when question_patient_answered.form_question_id = '118084'
                and question_patient_answered.answer_as_string is not null
                then 1
            else 0
        end) as ahq_sexually_active_answered_ind
    from {{ ref('question_patient_answered') }} as question_patient_answered
    inner join cohort
        on cohort.pat_key = question_patient_answered.pat_key
        and cohort.visit_key = question_patient_answered.visit_key
    where
        /*'WEL CN ADOLESCENT HEALTH QUESTIONNAIRE'*/
        question_patient_answered.form_id = '100179'
        /*'WEL CN ADOLESCENT HEALTH QUESTIONNAIRE-SEXUALLY ACTIVE'*/
        or question_patient_answered.form_id = '100181'
    group by
        question_patient_answered.visit_key,
        question_patient_answered.pat_key,
        question_patient_answered.encounter_date,
        ahq_given_ind
),

stg_test_exclusion as (
-- region test_exclusion:
-- pulling in data from smart data element for teens who are excluded from STI testing denominator
-- based on decline acknowledgement on STI BPA
    select
        visit_key,
        encounter_date,
        concept_id,
        element_value,
        concept_key,
        sde_entered_employee,
        entered_date,
        date(entered_date) - encounter_date as encounter_sde_lag_days,
        case
            when element_value in (
                'Test completed within past 1 year',
                'AHQ responses do not necessitate testing',
                'Test supplies not available',
                'Parent / guardian DECLINES testing',
                'Confidentiality concerns',
                'Patient DECLINES testing'
            ) then 1 else 0
        end as sti_test_exclusion,
        case
            when element_value = 'Testing ORDERED'
            then 1
            else 0
        end as sti_test_ordered
    from {{ ref('smart_data_element_all') }}
    where lower(concept_id) in ('chopcn#428', 'chopcn#429')
    and encounter_date >= '2022-09-22' -- date when smart data element became available for use
    and encounter_date < (current_date - 6)
),

test_exclusion as (
    select
        visit_key,
        encounter_date,
        max(sti_test_exclusion) as sti_test_exclusion,
        max(sti_test_ordered) as sti_test_ordered
    from stg_test_exclusion
    group by
        visit_key,
        encounter_date
),

completed_test_results as (
--region test: look for test results with the components of interest to determine if the test was performed
    select
        procedure_order_result_clinical.proc_ord_key,
        procedure_order_result_clinical.encounter_date as completed_test_encounter_date,
        cohort.encounter_date as cohort_encounter_date,
        months_between(cohort.encounter_date, procedure_order_result_clinical.encounter_date)
            as months_between_encounters,
        procedure_order_result_clinical.result_component_name,
        procedure_order_result_clinical.result_component_id,
        procedure_order_result_clinical.placed_date,
        procedure_order_result_clinical.specimen_taken_date,
        procedure_order_result_clinical.result_value,
        procedure_order_result_clinical.department_name,
        procedure_order_result_clinical.result_date,
        procedure_order_result_clinical.pat_key,
        procedure_order_result_clinical.visit_key as completed_test_visit_key,
        cohort.visit_key as cohort_visit_key,
        /*Was testing completed*/
        case
            when upper(procedure_order_result_clinical.result_component_name) like '%TRACHOMATIS%'
            or upper(procedure_order_result_clinical.result_component_name) like '%C.TRACH%'
            or upper(procedure_order_result_clinical.result_component_name) like '%CHLAMYDIA%'
            then 1 else 0
        end as chlamydia_test_ind,
        coalesce(
            lag(completed_test_visit_key) over (
                partition by procedure_order_result_clinical.pat_key
                order by completed_test_encounter_date
            ), completed_test_visit_key
        ) as last_completed_test_visit,
        coalesce(
            lag(completed_test_encounter_date) over (
                partition by procedure_order_result_clinical.pat_key
                order by completed_test_encounter_date
            ), completed_test_encounter_date
        ) as last_completed_test_encounter_date,
        /*indicator for when the test was ordered by one of the primary care sites*/
        /*Was the test positive*/
        case
            when (
                    chlamydia_test_ind = 1
                    and (
                            (
                                upper(procedure_order_result_clinical.result_value) like '%NEG%'
                                or upper(procedure_order_result_clinical.result_value) like 'NO%'
                            )
                        and upper(procedure_order_result_clinical.result_value) not in (
                            'NOT DONE',
                            'NOT AVAIL.'
                            )
                        )
                ) then 0
            when (
                    chlamydia_test_ind = 1
                    and (
                        upper(procedure_order_result_clinical.result_value) like '%POSITIVE%'
                        or upper(procedure_order_result_clinical.result_value) like 'DETECTED%'
                        or upper(procedure_order_result_clinical.result_value) like
                            '[CHLAMYDIA TRACHOMATIS] ISOLATED%'
                    )
                ) then 1
            else null
        end as chlamydia_positive_ind
    from {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
    inner join cohort
        on cohort.pat_key = procedure_order_result_clinical.pat_key
        and (months_between(cohort.encounter_date, procedure_order_result_clinical.encounter_date) < 12
            or procedure_order_result_clinical.encounter_date > cohort.encounter_date)
    where chlamydia_test_ind = 1
)

select
    cohort.visit_key,
    cohort.pat_key,
    cohort.sex,
    coalesce(adolescent_health_questionnaire.ahq_given_ind, 0) as ahq_given_ind,
    adolescent_health_questionnaire.ahq_sexually_active_ind,
    coalesce(test_exclusion.sti_test_exclusion, 0) as refusal_sti_visit_ind,
    completed_test_results.chlamydia_test_ind,
    case
        when ahq_sexually_active_ind = 1
        and (
                refusal_sti_visit_ind != 1
                or (
                        refusal_sti_visit_ind = 1
                        and completed_test_results.chlamydia_test_ind = 1
                        and completed_test_visit_key = cohort_visit_key
                )
        )
        then 1
        else 0
    end as eligible_sti_visit_pc_ind,
    case
        when ahq_sexually_active_ind = 1
        and cohort.sex = 'F'
        then 1
        else 0
    end as eligible_sti_visit_eop_ind,
    case
        when completed_test_results.chlamydia_test_ind = 1
        and completed_test_encounter_date = cohort_encounter_date
        then 1
        else 0
    end as chlamydia_test_visit_ind,
    case
        when completed_test_results.chlamydia_positive_ind = 1
        and completed_test_encounter_date = cohort_encounter_date
        then 1
        else 0
    end as chlamydia_positive_visit_ind,
    case
        when completed_test_results.chlamydia_test_ind = 1
        and months_between_encounters > 0
        then 1
        else 0
    end as chlamydia_test_past_yr_ind,
    case
        when completed_test_results.chlamydia_positive_ind = 1
        and months_between_encounters > 0
        then 1
        else 0
    end as chlamydia_positive_past_yr_ind,
    case
        when chlamydia_test_visit_ind = 1
        then cohort_encounter_date
        when chlamydia_positive_past_yr_ind = 1
            and completed_test_encounter_date = cohort_encounter_date
        then completed_test_results.placed_date::date
        when chlamydia_test_past_yr_ind = 1
            then completed_test_results.last_completed_test_encounter_date
        else null
    end as chlamydia_last_test_date
from cohort
left join adolescent_health_questionnaire
    on adolescent_health_questionnaire.visit_key = cohort.visit_key
left join test_exclusion
    on adolescent_health_questionnaire.visit_key = test_exclusion.visit_key
    and test_exclusion.encounter_date = adolescent_health_questionnaire.encounter_date
left join completed_test_results
    on completed_test_results.pat_key = cohort.pat_key
    and completed_test_results.completed_test_encounter_date = cohort.encounter_date
