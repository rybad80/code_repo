select
    /*multiple rows per file_id*/
    feedback_cases.file_id,
    feedback_cases.feedback_id,
    feedback_cases.feedback_method,
    kaps_feedback_user_defined_field.primary_issue_classification as primary_classification,
    kaps_feedback_theme.feedback_theme,
    kaps_feedback_category.feedback_category,
    kaps_feedback_sub_category.subcategory as feedback_subcategory,
    kaps_feedback_target.target_type,
    feedback_cases.feedback_department,
    feedback_cases.feedback_building,
    feedback_cases.care_service_area,
    feedback_cases.feedback_description,
    /*enterprise leader/site where case is handled*/
    feedback_cases.primary_file_owner,
    feedback_cases.feedback_date_received,
    feedback_cases.feedback_date_entered,
    feedback_cases.feedback_date_closed,
    feedback_cases.file_status,
    feedback_cases.file_state,
    /*office of feedback staff member that created KAPS file*/
    feedback_cases.feedback_owner_name,
--    feedback_cases.pat_id,
    feedback_cases.pat_key,
    'KAPS' as create_by
from
    {{source('cdw', 'kaps_feedback_cases')}} as kaps_feedback_cases
        left join {{source('cdw', 'kaps_feedback_user_defined_field')}} as kaps_feedback_user_defined_field
            on kaps_feedback_cases.feedback_id = kaps_feedback_user_defined_field.feedback_id
        left join {{source('cdw', 'kaps_feedback_issue')}} as kaps_feedback_issue
            on kaps_feedback_cases.feedback_id = kaps_feedback_issue.feedback_id
        left join {{source('cdw', 'kaps_feedback_target')}} as kaps_feedback_target
            on kaps_feedback_issue.issue_id = kaps_feedback_target.issue_id
        left join {{source('cdw', 'kaps_feedback_sub_category')}} as kaps_feedback_sub_category
            on kaps_feedback_target.target_id = kaps_feedback_sub_category.target_id
        left join {{source('cdw', 'kaps_feedback_category')}} as kaps_feedback_category
            on kaps_feedback_target.target_id = kaps_feedback_category.target_id
        left join {{ref('feedback_cases')}} as feedback_cases
            on feedback_cases.file_id = kaps_feedback_cases.file_id
        left join {{source('cdw', 'kaps_feedback_theme')}} as kaps_feedback_theme
            on kaps_feedback_theme.feedback_id = kaps_feedback_cases.feedback_id
where
/*office of feedback wants to exclude all old data not found in RL*/
    feedback_date_received >= '2019-01-01'
