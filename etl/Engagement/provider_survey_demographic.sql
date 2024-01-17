/* provider demographic */
select
    '2019' as survey_year,
    null as cost_center,
    null as cost_center_id,
    primary_work_setting as work_setting,
    null as care_setting,
    location as work_location,
    job_position as primary_responsibilities_category,
    physician_medical_specialty as board_certification,
    division as primary_affiliated_division,
    null as clinical_areas,
    please_select_your_section as department,
    null as provider_role,
    chop_provider_role,
    primary_provider_business_relationship as business_relationship,
    primary_provider_business_relationship_1 as provider_affiliate_relationship,
    years_of_hospital_affiliation as years_of_affiliation,
    null as years_of_practice,
    generation,
    sex as gender_identity,
    null as identify_as_transgender,
    null as lgbtq,
    null as legacy_race,
    race,
    null as ethnicity,
    null as bias_or_disrespect_from_coworkers,
    null as bias_or_disrespect_from_patients_guests,
    null as penn_faculty,
    null as academic_track,
    null as academic_rank,
    null as likely_to_retire_move_out_of_your_current_region_or_go_back_to_school_full_time_in_the_next_3_years,
    case
        when provide_care_in_emergency_department like 'Yes%' then 1
        when provide_care_in_emergency_department like 'No%' then 0
        end as provide_ed_care_ind,
    case
        when hospitalist like 'Yes%' then 1
        when hospitalist like 'No%' then 0
        end as hospitalist_ind,
    null as research_ind,
    null as contact_id,
    null as email,
    null as name,
    null as first_name,
    null as last_name,
    manager_first_name,
    manager_last_name,
    manager_id,
    department_chair,
    prov_survey_demographic_2019.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_demographic_2019')}} as prov_survey_demographic_2019
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_demographic_2019.question = lookup.question_text_2019
union all
select
    '2020' as survey_year,
    cost_center,
    null as cost_center_id,
    primary_work_setting as work_setting,
    null as care_setting,
    location as work_location,
    case
        when job_position like 'Other licensed providers%'
        then 'Other licensed providers'
        else job_position end as primary_responsibilities_category,
    physician_medical_specialty as board_certification,
    what_division_do_you_primarily_affiliate_with as primary_affiliated_division,
    null as clinical_areas,
    department,
    null as provider_role,
    chop_provider_role,
    primary_provider_business_relationship as business_relationship,
    null as provider_affiliate_relationship,
    years_of_hospital_affiliation as years_of_affiliation,
    provider_practice_range as years_of_practice,
    generation,
    gender_identity,
    null as identify_as_transgender,
    null as lgbtq,
    null as legacy_race,
    race,
    case
        when are_you_of_hispanic_latino_or_spanish_origin like 'Yes'
        then 'Hispanic or Latino'
        when are_you_of_hispanic_latino_or_spanish_origin like 'No'
        then 'Non-Hispanic or Latino'
        end as ethnicity,
    null as bias_or_disrespect_from_coworkers,
    null as bias_or_disrespect_from_patients_guests,
    are_you_penn_faculty as penn_faculty,
    please_select_your_academic_track as academic_track,
    what_is_your_academic_rank as academic_rank,
    null as likely_to_retire_move_out_of_your_current_region_or_go_back_to_school_full_time_in_the_next_3_years,
    case
        when provide_care_in_emergency_department like 'Yes%' then 1
        when provide_care_in_emergency_department like 'No%' then 0
        end as provide_ed_care_ind,
    case
        when hospitalist like 'Yes%' then 1
        when hospitalist like 'No%' then 0
        end as hospitalist_ind,
    null as research_ind,
    null as contact_id,
    null as email,
    null as name,
    null as first_name,
    null as last_name,
    manager_first_name,
    manager_last_name,
    manager_id,
    department_chair,
    prov_survey_demographic_2020.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_demographic_2020')}} as prov_survey_demographic_2020
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_demographic_2020.question = lookup.question_text_2020
union all
select
    '2021' as survey_year,
    cost_center,
    null as cost_center_id,
    work_setting,
    null as care_setting,
    location as work_location,
    primary_responsibilities_use_with_ph10013_primary_responsibilities
        as primary_responsibilities_category,
    board_certification,
    what_division_do_you_primarily_affiliate_with as primary_affiliated_division,
    clinical_areas,
    department,
    null as provider_role,
    please_select_your_job_title as chop_provider_role,
    null as business_relationship,
    business_relationship as provider_affiliate_relationship,
    years_of_affiliation,
    years_of_practice,
    generation,
    gender_identity,
    do_you_identify_as_transgender as identify_as_transgender,
    null as lgbtq,
    legacy_race,
    race,
    ethnicity,
    null as bias_or_disrespect_from_coworkers,
    null as bias_or_disrespect_from_patients_guests,
    are_you_penn_faculty as penn_faculty,
    please_select_your_academic_track as academic_track,
    what_is_your_academic_rank as academic_rank,
    are_you_likely_to_retire_move_out_of_your_current_region_or_go_back_to_school_full_time_in_the_next_three_years --noqa: L016
        as likely_to_retire_move_out_of_your_current_region_or_go_back_to_school_full_time_in_the_next_3_years,
    case
        when provide_ed_care like 'Yes%' then 1
        when provide_ed_care like 'No%' then 0
        end as provide_ed_care_ind,
    case
        when hospitalist like 'Yes%' then 1
        when hospitalist like 'No%' then 0
        end as hospitalist_ind,
    null as research_ind,
    contact_id,
    email,
    name,
    first_name,
    last_name,
    manager_first_name,
    manager_last_name,
    manager_id,
    department_chair,
    prov_survey_demographic_2021.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_demographic_2021')}} as prov_survey_demographic_2021
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_demographic_2021.question = lookup.question_text_2021
union all
select
    '2022' as survey_year,
    cost_center,
    cost_center_id, -- new
    work_setting,
    care_setting, -- new
    location as work_location,
    job_position
        as primary_responsibilities_category,
    board_certification,
    what_division_do_you_primarily_affiliate_with as primary_affiliated_division,
    clinical_areas,
    department,
    provider_role, -- new
    please_select_your_job_title as chop_provider_role,
    business_relationship as business_relationship,
    null as provider_affiliate_relationship,
    years_of_affiliation,
    years_of_practice,
    generation,
    gender_identity,
    null as identify_as_transgender,
    lgbtq, -- new
    null as legacy_race,
    race,
    ethnicity,
    bias_or_disrespect_from_coworkers, -- new
    bias_or_disrespect_from_patients_guests, -- new
    penn_faculty,
    please_select_your_academic_track as academic_track,
    what_is_your_academic_rank as academic_rank,
    are_you_likely_to_retire_move_out_of_your_current_region_or_go_back_to_school_full_time_in_the_next_three_years --noqa: L016
        as likely_to_retire_move_out_of_your_current_region_or_go_back_to_school_full_time_in_the_next_3_years,
    case
        when provide_ed_care like 'Yes%' then 1
        when provide_ed_care like 'No%' then 0
        end as provide_ed_care_ind,
    case
        when hospitalist like 'Yes%' then 1
        when hospitalist like 'No%' then 0
        end as hospitalist_ind,
    case
        when research like 'Yes%' then 1
        when research like 'No%' then 0
        end as research_ind, -- new
    null as contact_id,
    null as email,
    null as name,
    null as first_name,
    null as last_name,
    null as manager_first_name,
    null as manager_last_name,
    null as manager_id,
    department_chair,
    prov_survey_demographic_2022.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_demographic_2022')}} as prov_survey_demographic_2022
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_demographic_2022.question = lookup.question_text_2022
