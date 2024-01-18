/* employee demographic */
select
    '2019' as survey_year,
    cost_center,
    please_select_your_primary_work_location as work_location,
    please_select_the_category_that_best_describes_your_primary_responsibilities_greater_than_50_of_your_time
        as primary_responsibilities_category,
    please_select_your_job_classification as job_classification,
    please_select_your_employment_status as employment_status,
    null as clinical_division,
    null as job_position,
    please_select_your_job_title as job_title,
    please_select_your_shift as shift,
    null as hire_year,
    please_select_your_length_of_service as length_of_service,
    please_select_your_generation as generation,
    null as birth_year,
    please_select_your_age as age,
    please_select_your_sex as gender_identity,
    null as identify_as_transgender,
    null as lgbtq,
    null as legacy_race,
    please_select_your_race as race,
    null as ethnicity,
    null as bias_or_disrespect_from_coworkers,
    null as plan_to_leave_chop_in_the_next_2_years,
    null as reason_to_plan_to_leave_chop_in_the_next_2_years,
    case
        when do_you_spend_at_least_50_of_your_time_in_direct_patient_care like 'Yes%' then 1
        when do_you_spend_at_least_50_of_your_time_in_direct_patient_care like 'No%' then 0
        end as direct_patient_care_ind,
    case
        when regardless_of_the_position_you_currently_hold_at_chop_are_you_a_registered_nurse like 'Yes%' then 1
        when regardless_of_the_position_you_currently_hold_at_chop_are_you_a_registered_nurse like 'No%' then 0
        end as registered_nurse_ind,
    null as research_ind,
    null as remote_worker_ind,
    null as union_member_ind,
    null as contact_id,
    null as email,
    null as name,
    null as first_name,
    null as last_name,
    null as nurse_manager_name,
    emp_survey_demographic_2019.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_demographic_2019')}} as emp_survey_demographic_2019
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_demographic_2019.question = lookup.question_text_2019
union all
select
    '2020' as survey_year,
    cost_center,
    location as work_location,
    please_select_the_category_that_best_describes_your_primary_responsibilities_greater_than_50_of_your_time
        as primary_responsibilities_category,
    job_classification,
    employment_status,
    null as clinical_division,
    job_position,
    job_title,
    shift,
    hire_year,
    length_of_service,
    generation,
    birth_year,
    age,
    gender_identity,
    null as identify_as_transgender,
    null as lgbtq,
    null as legacy_race,
    case when race like 'White%' then 'White' else race end as race,
    case
        when are_you_of_hispanic_latino_or_spanish_origin like 'Yes'
        then 'Hispanic or Latino'
        when are_you_of_hispanic_latino_or_spanish_origin like 'No'
        then 'Non-Hispanic or Latino'
        end as ethnicity,
    null as bias_or_disrespect_from_coworkers,
    do_you_plan_to_leave_chop_in_the_next_2_years
        as plan_to_leave_chop_in_the_next_2_years,
    what_is_the_reason_you_plan_to_leave_chop_in_the_next_2_years
        as reason_to_plan_to_leave_chop_in_the_next_2_years,
    case
        when direct_patient_care like 'Yes%' then 1
        when direct_patient_care like 'No%' then 0
        end as direct_patient_care_ind,
    case
        when registered_nurse like 'Yes%' then 1
        when registered_nurse like 'No%' then 0
        end as registered_nurse_ind,
    null as research_ind,
    null as remote_worker_ind,
    null as union_member_ind,
    null as contact_id,
    null as email,
    null as name,
    null as first_name,
    null as last_name,
    null as nurse_manager_name,
    emp_survey_demographic_2020.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_demographic_2020')}} as emp_survey_demographic_2020
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_demographic_2020.question = lookup.question_text_2020
union all
select
    '2021' as survey_year,
    cost_center_hierarchy as cost_center,
    location as work_location,
    please_select_the_category_that_best_describes_your_primary_responsibilities_greater_than_50_of_your_time
        as primary_responsibilities_category,
    job_classification,
    employment_status,
    please_select_from_the_list_below as clinical_division,
    job_position,
    job_title,
    shift,
    null as hire_year,
    length_of_service,
    generation,
    null as birth_year,
    age,
    gender_identification as gender_identity,
    do_you_identify_as_transgender as identify_as_transgender,
    null as lgbtq,
    legacy_race,
    race,
    ethnicity,
    null as bias_or_disrespect_from_coworkers,
    do_you_plan_to_leave_chop_in_the_next_2_years
        as plan_to_leave_chop_in_the_next_2_years,
    what_is_the_reason_you_plan_to_leave_chop_in_the_next_2_years
        as reason_to_plan_to_leave_chop_in_the_next_2_years,
    case
        when direct_patient_care like 'Yes%' then 1
        when direct_patient_care like 'No%' then 0
        end as direct_patient_care_ind,
    case
        when registered_nurse like 'Yes%' then 1
        when registered_nurse like 'No%' then 0
        end as registered_nurse_ind,
    null as research_ind,
    case
        when remote like 'Yes%' then 1
        when remote like 'No%' then 0
        end as remote_worker_ind,
    null as union_member_ind,
    contact_id,
    email,
    name,
    first_name,
    last_name,
    nurse_manager_name,
    emp_survey_demographic_2021.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_demographic_2021')}} as emp_survey_demographic_2021
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_demographic_2021.question = lookup.question_text_2021
union all
select
    '2022' as survey_year,
    cost_center,
    location as work_location,
    please_select_the_category_that_best_describes_your_primary_responsibilities_greater_than_50_percent_of_your_time -- noqa: L016
        as primary_responsibilities_category,
    job_classification,
    employment_status,
    please_select_from_the_list_below as clinical_division,
    please_select_the_category_that_best_describes_your_primary_responsibilities_greater_than_50_percent_of_your_time -- noqa: L016
        as job_position,
    job_title,
    shift,
    null as hire_year,
    length_of_service,
    generation,
    null as birth_year,
    age,
    gender_identification as gender_identity,
    null as identify_as_transgender,
    lgbtq,
    null as legacy_race,
    race,
    ethnicity,
    bias_or_disrespect_from_coworkers,
    do_you_plan_to_leave_chop_in_the_next_2_years
        as plan_to_leave_chop_in_the_next_2_years,
    what_is_the_reason_you_plan_to_leave_chop_in_the_next_2_years
        as reason_to_plan_to_leave_chop_in_the_next_2_years,
    case
        when direct_patient_care like 'Yes%' then 1
        when direct_patient_care like 'No%' then 0
        end as direct_patient_care_ind,
    case
        when registered_nurse like 'Yes%' then 1
        when registered_nurse like 'No%' then 0
        end as registered_nurse_ind,
    case
        when research like 'Yes%' then 1
        when research like 'No%' then 0
        end as research_ind,
    case
        when remote like 'Yes%' then 1
        when remote like 'No%' then 0
        end as remote_worker_ind,
    case
        when union_member like 'Yes%' then 1
        when union_member like 'No%' then 0
        end as union_member_ind,
    null as contact_id,
    null as email,
    null as name,
    null as first_name,
    null as last_name,
    null as nurse_manager_name,
    emp_survey_demographic_2022_exempt.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_demographic_2022_exempt')}} as emp_survey_demographic_2022_exempt
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_demographic_2022_exempt.question = lookup.question_text_2022
union all
select
    '2022' as survey_year,
    cost_center,
    location as work_location,
    please_select_the_category_that_best_describes_your_primary_responsibilities_greater_than_50_percent_of_your_time -- noqa: L016
        as primary_responsibilities_category,
    job_classification,
    employment_status,
    please_select_from_the_list_below as clinical_division,
    please_select_the_category_that_best_describes_your_primary_responsibilities_greater_than_50_percent_of_your_time -- noqa: L016
        as job_position,
    job_title,
    shift,
    null as hire_year,
    length_of_service,
    generation,
    null as birth_year,
    age,
    gender_identification as gender_identity,
    null as identify_as_transgender,
    lgbtq,
    null as legacy_race,
    race,
    ethnicity,
    bias_or_disrespect_from_coworkers,
    do_you_plan_to_leave_chop_in_the_next_2_years
        as plan_to_leave_chop_in_the_next_2_years,
    what_is_the_reason_you_plan_to_leave_chop_in_the_next_2_years
        as reason_to_plan_to_leave_chop_in_the_next_2_years,
    case
        when direct_patient_care like 'Yes%' then 1
        when direct_patient_care like 'No%' then 0
        end as direct_patient_care_ind,
    case
        when registered_nurse like 'Yes%' then 1
        when registered_nurse like 'No%' then 0
        end as registered_nurse_ind,
    case
        when research like 'Yes%' then 1
        when research like 'No%' then 0
        end as research_ind,
    case
        when remote like 'Yes%' then 1
        when remote like 'No%' then 0
        end as remote_worker_ind,
    case
        when union_member like 'Yes%' then 1
        when union_member like 'No%' then 0
        end as union_member_ind,
    null as contact_id,
    null as email,
    null as name,
    null as first_name,
    null as last_name,
    null as nurse_manager_name,
    emp_survey_demographic_2022_non_exempt.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_demographic_2022_non_exempt')}} as emp_survey_demographic_2022_non_exempt
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_demographic_2022_non_exempt.question = lookup.question_text_2022
