select
    kaps_claim_file.file_id,
    kaps_feedback_follow_up.feedback_id,
    kaps_feedback_subject.file_number as mrn,
    kaps_feedback_cases.submission_dt as feedback_date_received,
    kaps_claim_file.enter_dt as feedback_date_entered,
    kaps_feedback_user_defined_field.write_off_close_dt as feedback_date_closed,
    kaps_feedback_follow_up.feedback_follow_up_dt::date
        + kaps_feedback_follow_up.feedback_follow_up_tm::time as datetime_followup,
    kaps_feedback_follow_up.feedback_follow_up_type as type_followup,
    kaps_feedback_follow_up.feedback_follow_up_sub_type as subtype_followup,
    kaps_feedback_follow_up.feedback_follow_up_method as method_followup,
    kaps_feedback_follow_up.feedback_follow_up_to,
    kaps_feedback_follow_up.details as feedback_details
from
    {{source('cdw', 'kaps_feedback_follow_up')}} as kaps_feedback_follow_up
        left join {{source('cdw', 'kaps_feedback_subject')}} as kaps_feedback_subject
            on kaps_feedback_follow_up.feedback_id = kaps_feedback_subject.feedback_id
    left join {{source('cdw', 'kaps_feedback_cases')}} as kaps_feedback_cases
        on kaps_feedback_follow_up.feedback_id = kaps_feedback_cases.feedback_id
    left join {{source('cdw', 'kaps_claim_file')}} as kaps_claim_file
        on kaps_feedback_cases.file_id = kaps_claim_file.file_id
    left join {{source('cdw', 'kaps_master_module')}} as kaps_master_module
        on kaps_claim_file.mstr_module_key = kaps_master_module.mstr_module_key
    left join {{source('cdw', 'kaps_file_state')}} as kaps_file_state
        on kaps_claim_file.file_id = kaps_file_state.file_id
    left join {{source('cdw', 'kaps_feedback_user_defined_field')}} as kaps_feedback_user_defined_field
        on kaps_feedback_cases.feedback_id = kaps_feedback_user_defined_field.feedback_id
where
    kaps_master_module.module_id = 2
    and lower(kaps_file_state.file_state) not in ('deleted-inc', 'deleted', 'incomplete')
