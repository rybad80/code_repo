{{ config(meta = {
    'critical': true
}) }}

with elect_smart_form as (
    select distinct
        smart_data_element_info.pat_key
    from
        {{source('cdw', 'smart_data_element_info')}} as smart_data_element_info
    where
        smart_data_element_info.src_sys_val = 'SmartForm 578' --CHOP ENDO ELECT INTAKE FORM
        or smart_data_element_info.src_sys_val = 'SmartForm 786'--CHOP ENDO ELECT AXES NOTEWRITER
)
select
    scheduling_specialty_care_appointments.visit_key,
    1 as elect_ind
from
    {{ref('scheduling_specialty_care_appointments')}} as scheduling_specialty_care_appointments
where
    scheduling_specialty_care_appointments.encounter_date > to_date('2019-07-01', 'yyyy-mm-dd')
    and ( scheduling_specialty_care_appointments.visit_type_id = '2554' --ENDOCRINE LATE EFFECTS NEW
        or scheduling_specialty_care_appointments.visit_type_id = '2558' --ENDO LATE EFFECTS FOLLOW UP
        or (lower(scheduling_specialty_care_appointments.specialty_name) = 'endocrinology'
            and scheduling_specialty_care_appointments.video_telephone_visit_ind = 1
            and scheduling_specialty_care_appointments.pat_key in (select * from elect_smart_form)
        )
    )
